# -*- coding: utf-8 -*-
"""************************************************************
### Author: Zeng Shengbo shengbo.zeng@accelbio.ai
### Date: 12/27/2025 20:52:19
### LastEditors: Zeng Shengbo shengbo.zeng@accelbio.ai
### LastEditTime: 11/9/2026 10:15:05
### FilePath: //pubchem//nih//pubchem//index//sdf_index.py
### Description:
基于 LMDB 的 PubChem SDF 偏移索引读取与底层写入接口。

本模块定义 ``SDFIndex`` 和 ``IndexHit``。索引仅保存原始 SDF 记录所在文件及其
字节区间，不复制记录正文；查询命中后可通过 ``read_segment`` 回到原始文件读取
完整记录。支持的访问方式包括 ALID、Compound CID、Conformer ID，以及从 CID
流式遍历其全部 Conformer。

``SDFIndex`` 同时提供少量以下划线开头的写入方法，供 ``SDFIndexBuilder`` 在
构建阶段使用。业务调用方应以只读模式打开已经构建完成的索引。

###
### Copyright (c) 2025 by AccelBio.AI, All Rights Reserved.
**********************************************************"""
from __future__ import annotations

import json
import uuid
import lmdb
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional, Dict, Tuple, Union
from nih.pubchem.index.record_locator import RecordLocator
from nih.pubchem.index.utils_module import _chunked


@dataclass(frozen=True)
class IndexHit:
    """一次索引查询的命中结果。

    Attributes:
        alid: 记录的确定性 UUIDv5 标识。
        locator: 记录在原始 SDF 文件中的字节定位信息。

    该对象不包含 SDF 正文；需要将 ``locator`` 传给 ``SDFIndex.read_segment``
    才能读取原始记录。
    """

    alid: uuid.UUID
    locator: RecordLocator


# ---------------------------------------------------------------------------
# 核心索引类
# ---------------------------------------------------------------------------


class SDFIndex:
    """打开、查询或辅助构建一个 LMDB SDF 偏移索引。

    Args:
        index_dir: LMDB 环境所在目录。
        readonly: 是否以只读模式打开。查询场景应保持 ``True``；构建器会传入
            ``False`` 以创建数据库并写入记录。
        map_size: LMDB 映射空间上限，单位为字节，默认 1 TiB。该值是地址空间
            上限，并不表示立即分配同等磁盘空间。

    Attributes:
        index_dir: 规范为 ``Path`` 的索引目录。
        readonly: 当前索引实例的只读标志。
        env: 已打开的 LMDB 环境。

    LMDB 子数据库：
        meta: JSON 元数据、schema 版本和文件编号计数器。
        files: ``file_id -> 相对路径``。
        files_rev: ``相对路径 -> file_id``。
        records: ``b"C" + uuid16`` 或 ``b"F" + uuid16`` 到定位信息。
        cid_to_compound: ASCII CID 到 Compound 主记录键。
        confid_to_conf: UTF-8 Conformer ID 到 Conformer 主记录键。
        cid_to_conformers_h: CID 到 posting list 页数。
        cid_to_conformers_p: ``CID|页号`` 到连续拼接的 UUID16 列表。
    """

    # 索引格式版本；构建时写入 meta，便于未来进行兼容性判断。
    SCHEMA_VERSION = 1

    def __init__(
        self,
        index_dir: Union[str, Path],
        readonly: bool = True,
        map_size: int = 1 << 40,
    ):
        """初始化索引对象并打开所需的 LMDB 子数据库。

        Args:
            index_dir: LMDB 环境目录。
            readonly: ``True`` 时禁止创建和写入；``False`` 时允许构建索引。
            map_size: LMDB 最大映射空间，单位为字节。

        Raises:
            lmdb.Error: 索引目录不存在、权限不足、格式错误或 LMDB 打开失败时
                抛出。
        """
        # 第 1 步：保存规范化后的构造参数，供查询和构建器复用。
        self.index_dir = Path(index_dir)
        self.readonly = readonly

        # 第 2 步：根据只读标志配置 LMDB。只读模式启用 readahead 且不创建目录；
        # 写模式启用锁、元数据同步和数据同步，确保构建过程可持久化。
        self.env = lmdb.open(
            str(self.index_dir),
            readonly=readonly,
            lock=not readonly,
            readahead=readonly,
            map_size=map_size,
            max_dbs=32,
            subdir=True,
            create=not readonly,
            metasync=not readonly,
            sync=not readonly,
        )

        # 第 3 步：打开全部命名子数据库，并保存句柄供后续事务访问。
        self.db_meta = self.env.open_db(b"meta")
        self.db_files = self.env.open_db(b"files")
        self.db_files_rev = self.env.open_db(b"files_rev")
        self.db_records = self.env.open_db(b"records")
        self.db_cid_to_compound = self.env.open_db(b"cid_to_compound")
        self.db_confid_to_conf = self.env.open_db(b"confid_to_conf")
        self.db_cid2conf_h = self.env.open_db(b"cid_to_conformers_h")
        self.db_cid2conf_p = self.env.open_db(b"cid_to_conformers_p")

    # ------------------------------ 元数据 ------------------------------

    def get_meta(self) -> Dict:
        """读取索引的 JSON 元数据。

        Returns:
            元数据字典；尚未写入 ``meta_json`` 时返回空字典。通常包含 schema
            版本、原始数据根目录、构建时间和记录统计。
        """
        # 使用短生命周期只读事务获取单一元数据值。
        with self.env.begin(db=self.db_meta) as txn:
            b = txn.get(b"meta_json")
            if not b:
                return {}
            # LMDB 保存 UTF-8 JSON 字节串，这里恢复为 Python 字典。
            return json.loads(b.decode("utf-8"))

    def _set_meta(self, meta: Dict) -> None:
        """将索引元数据序列化为 JSON 后写入 LMDB。

        Args:
            meta: 可被 ``json.dumps`` 序列化的元数据字典。

        Raises:
            RuntimeError: 当前索引以只读模式打开时抛出。
            TypeError: 字典中包含不可 JSON 序列化的值时抛出。

        Notes:
            该方法属于构建器内部接口，外部查询代码不应调用。
        """
        # 第 1 步：显式阻止只读实例进入写事务。
        if self.readonly:
            raise RuntimeError("Index is readonly")

        # 第 2 步：使用独立写事务覆盖 meta_json；sort_keys 使输出稳定。
        with self.env.begin(write=True, db=self.db_meta) as txn:
            txn.put(
                b"meta_json",
                json.dumps(meta, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            )

    # ------------------------------ 文件表 ------------------------------

    def _get_or_create_file_id(self, txn: lmdb.Transaction, relpath: str) -> int:
        """在给定写事务中取得或创建相对路径对应的文件编号。

        Args:
            txn: 已由调用方开启的 LMDB 写事务。文件表、反向文件表和计数器会
                在同一事务内更新。
            relpath: 原始 SDF 文件相对于数据根目录的路径，统一使用正斜杠。

        Returns:
            已存在或新分配的正整数 ``file_id``。

        Notes:
            该方法属于构建器内部接口。新编号通过 meta 中的单调计数器生成。
        """
        # 第 1 步：把路径编码为 LMDB 键，并优先查询反向文件表。
        k = relpath.encode("utf-8")
        existing = txn.get(k, db=self.db_files_rev)
        if existing:
            return int.from_bytes(existing, "little", signed=False)

        # 第 2 步：路径尚未登记时读取单调计数器；首次构建从 1 开始编号。
        counter_b = txn.get(b"file_id_counter", db=self.db_meta)
        counter = int.from_bytes(counter_b, "little") if counter_b else 0
        file_id = counter + 1

        # 第 3 步：在同一事务中同时更新计数器、路径反查表和编号正查表。
        txn.put(b"file_id_counter", file_id.to_bytes(8, "little"), db=self.db_meta)
        txn.put(k, file_id.to_bytes(8, "little"), db=self.db_files_rev)
        txn.put(file_id.to_bytes(8, "little"), k, db=self.db_files)
        return file_id

    def resolve_file_path(self, file_id: int) -> Optional[str]:
        """将内部文件编号解析为原始 SDF 相对路径。

        Args:
            file_id: 文件表中的整数编号。

        Returns:
            UTF-8 相对路径；编号不存在时返回 ``None``。
        """
        # 文件编号统一编码为 8 字节小端无符号整数后查询文件表。
        with self.env.begin(db=self.db_files) as txn:
            b = txn.get(int(file_id).to_bytes(8, "little"))
            return None if not b else b.decode("utf-8")

    # ------------------------------ 单条记录查询 ------------------------------

    def get_by_alid(
        self, alid: Union[str, uuid.UUID], is_conformer: Optional[bool] = None
    ) -> Optional[IndexHit]:
        """按 ALID 查询 Compound 或 Conformer 记录。

        Args:
            alid: UUID 字符串或 ``uuid.UUID`` 对象。
            is_conformer: 记录类型提示。``True`` 只查 Conformer，``False`` 只查
                Compound，``None`` 则先查 Compound、再查 Conformer。

        Returns:
            命中时返回 ``IndexHit``，否则返回 ``None``。

        Raises:
            ValueError: ``alid`` 字符串不是合法 UUID 时抛出。
        """
        # 第 1 步：把字符串形式的 ALID 统一转换为 UUID 对象。
        if isinstance(alid, str):
            alid_u = uuid.UUID(alid)
        else:
            alid_u = alid

        # 第 2 步：分别构造 Compound 和 Conformer 的主记录键。
        key_c = b"C" + alid_u.bytes
        key_f = b"F" + alid_u.bytes

        # 第 3 步：在 records 子库中按类型提示查询并解码定位信息。
        with self.env.begin(db=self.db_records) as txn:
            if is_conformer is None:
                # 类型未知时按照 Compound -> Conformer 的固定顺序尝试。
                b = txn.get(key_c)
                if b:
                    return IndexHit(alid_u, RecordLocator.from_bytes(b))
                b = txn.get(key_f)
                if b:
                    return IndexHit(alid_u, RecordLocator.from_bytes(b))
                return None

            # 类型已知时只访问对应前缀的键，避免多余查询。
            b = txn.get(key_f if is_conformer else key_c)
            if not b:
                return None
            return IndexHit(alid_u, RecordLocator.from_bytes(b))

    def get_compound_by_cid(self, cid: int) -> Optional[IndexHit]:
        """按 PubChem CID 查询唯一的 Compound 记录。

        Args:
            cid: 可转换为整数的 Compound CID。

        Returns:
            二级索引和主记录均存在时返回 ``IndexHit``，否则返回 ``None``。
        """
        # 第 1 步：将 CID 规范为十进制 ASCII，匹配构建时的二级索引键。
        k = str(int(cid)).encode("ascii")
        with self.env.begin() as txn:
            # 第 2 步：先从 CID 二级索引取得 records 子库中的完整主键。
            rec_key = txn.get(k, db=self.db_cid_to_compound)
            if not rec_key:
                return None

            # 第 3 步：用主键读取并解码 RecordLocator。
            b = txn.get(rec_key, db=self.db_records)
            if not b:
                return None

            # 主键首字节是类型前缀，后续 16 字节为 UUID 原始值。
            alid = uuid.UUID(bytes=rec_key[1:17])
            return IndexHit(alid, RecordLocator.from_bytes(b))

    def get_conformer_by_conformer_id(self, conformer_id: str) -> Optional[IndexHit]:
        """按 Conformer ID 查询唯一的 Conformer 记录。

        Args:
            conformer_id: 与 SDF 属性值完全一致的 Conformer ID 字符串。

        Returns:
            二级索引和主记录均存在时返回 ``IndexHit``，否则返回 ``None``。
        """
        # 第 1 步：Conformer ID 允许非数字内容，因此使用 UTF-8 编码作为键。
        k = conformer_id.encode("utf-8")
        with self.env.begin() as txn:
            # 第 2 步：从 ID 二级索引取得 Conformer 主记录键。
            rec_key = txn.get(k, db=self.db_confid_to_conf)
            if not rec_key:
                return None

            # 第 3 步：读取定位值，并从主键恢复 ALID。
            b = txn.get(rec_key, db=self.db_records)
            if not b:
                return None
            alid = uuid.UUID(bytes=rec_key[1:17])
            return IndexHit(alid, RecordLocator.from_bytes(b))

    def iter_conformers_by_cid(self, cid: int) -> Iterator[IndexHit]:
        """按 CID 流式遍历其关联的全部 Conformer。

        Args:
            cid: 父 Compound CID。

        Yields:
            按 posting list 页序和页内顺序产生 ``IndexHit``。不存在关联页时
            生成器直接结束。

        Notes:
            posting list 分页保存，每页是多个连续 UUID16，适合一个 CID 关联
            大量 Conformer 的场景。
        """
        # 第 1 步：将 CID 转为 posting list 头表使用的 ASCII 键。
        cid_k = str(int(cid)).encode("ascii")
        with self.env.begin() as txn:
            # 第 2 步：读取该 CID 的总页数；没有头记录表示没有关联项。
            h = txn.get(cid_k, db=self.db_cid2conf_h)
            if not h:
                return
            page_count = int.from_bytes(h, "little", signed=False)

            # 第 3 步：按页号依次读取拼接的 UUID 数据块。
            for page_no in range(page_count):
                pk = cid_k + b"|" + str(page_no).encode("ascii")
                blob = txn.get(pk, db=self.db_cid2conf_p)
                if not blob:
                    continue

                # 第 4 步：每 16 字节切分一个 UUID，并回查主记录定位信息。
                for i in range(0, len(blob), 16):
                    ubytes = blob[i : i + 16]
                    # 防御性处理异常的非完整 UUID 尾部。
                    if len(ubytes) < 16:
                        break
                    rec_key = b"F" + ubytes
                    rec_val = txn.get(rec_key, db=self.db_records)
                    if not rec_val:
                        continue
                    yield IndexHit(
                        uuid.UUID(bytes=ubytes), RecordLocator.from_bytes(rec_val)
                    )

    # ------------------------------ 高吞吐批量查询 ------------------------------

    def batch_get_compounds_by_cid(
        self,
        cids: Iterable[int],
        chunk_size: int = 50000,
    ) -> Iterator[Tuple[int, Optional[IndexHit]]]:
        """分块、流式批量查询多个 Compound CID。

        Args:
            cids: CID 可迭代对象；可传入列表或生成器。
            chunk_size: 每批处理的 CID 数量，默认 50000。

        Yields:
            ``(cid, hit_or_none)`` 二元组，顺序与输入一致。未命中或二级索引
            指向缺失主记录时，第二项为 ``None``。

        Notes:
            整个迭代过程复用一个只读事务，分块主要用于限制输入缓冲区大小。
        """
        # 第 1 步：在同一只读事务内处理所有分块，减少事务创建开销。
        with self.env.begin() as txn:
            for chunk in _chunked(cids, chunk_size):
                # 第 2 步：逐个执行“CID 二级索引 -> 主记录”的两级查询。
                for cid in chunk:
                    k = str(int(cid)).encode("ascii")
                    rec_key = txn.get(k, db=self.db_cid_to_compound)
                    if not rec_key:
                        yield int(cid), None
                        continue
                    rec_val = txn.get(rec_key, db=self.db_records)
                    if not rec_val:
                        yield int(cid), None
                        continue
                    alid = uuid.UUID(bytes=rec_key[1:17])
                    yield int(cid), IndexHit(alid, RecordLocator.from_bytes(rec_val))

    def batch_get_conformers_by_conformer_id(
        self,
        conformer_ids: Iterable[str],
        chunk_size: int = 50000,
    ) -> Iterator[Tuple[str, Optional[IndexHit]]]:
        """分块、流式批量查询多个 Conformer ID。

        Args:
            conformer_ids: Conformer ID 字符串可迭代对象。
            chunk_size: 每批处理的 ID 数量，默认 50000。

        Yields:
            ``(conformer_id, hit_or_none)`` 二元组，顺序与输入一致；未命中时
            第二项为 ``None``。
        """
        # 第 1 步：复用一个只读事务，并按 chunk_size 控制输入缓冲区。
        with self.env.begin() as txn:
            for chunk in _chunked(conformer_ids, chunk_size):
                # 第 2 步：逐个执行“Conformer ID 二级索引 -> 主记录”查询。
                for confid in chunk:
                    k = confid.encode("utf-8")
                    rec_key = txn.get(k, db=self.db_confid_to_conf)
                    if not rec_key:
                        yield confid, None
                        continue
                    rec_val = txn.get(rec_key, db=self.db_records)
                    if not rec_val:
                        yield confid, None
                        continue
                    alid = uuid.UUID(bytes=rec_key[1:17])
                    yield confid, IndexHit(alid, RecordLocator.from_bytes(rec_val))

    # ------------------------------ 原始记录读取 ------------------------------

    def read_segment(self, root_dir: Union[str, Path], locator: RecordLocator) -> bytes:
        """根据定位信息读取一条原始 SDF 记录。

        Args:
            root_dir: 当前原始 SDF 数据根目录。它应与构建索引时的目录结构一致。
            locator: 索引命中结果中的 ``RecordLocator``。

        Returns:
            从 ``start``（包含）到 ``end``（不包含）的原始字节串，通常包括
            记录终止符 ``$$$$``。

        Raises:
            KeyError: ``locator.file_id`` 在文件表中不存在时抛出。
            OSError: 原始文件不存在、无权读取或读取失败时抛出。
        """
        # 第 1 步：通过文件表把内部编号解析为相对路径。
        rel = self.resolve_file_path(locator.file_id)
        if not rel:
            raise KeyError(f"file_id={locator.file_id} not found in index")

        # 第 2 步：把调用方提供的当前根目录与索引中的相对路径组合。
        fp = Path(root_dir) / rel

        # 第 3 步：以二进制方式定位起始偏移，并读取精确长度的记录字节。
        with fp.open("rb") as f:
            f.seek(locator.start)
            return f.read(locator.end - locator.start)
