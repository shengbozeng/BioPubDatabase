# -*- coding: utf-8 -*-
''' ************************************************************ 
### Author: Zeng Shengbo shengbo.zeng@accelbio.ai
### Date: 12/27/2025 20:58:50
### LastEditors: Zeng Shengbo shengbo.zeng@accelbio.ai
### LastEditTime: 11/9/2026 10:15:05
### FilePath: //pubchem//nih//pubchem//index//sdf_index_builder.py
### Description: 
扫描 PubChem SDF 文件并构建 LMDB 字节偏移索引。

构建器以二进制流方式逐行读取 SDF，不把完整文件或全部记录加载到内存。每遇到
``$$$$`` 记录终止符，就保存该记录的文件编号、起止字节偏移、记录类型和 CID，
同时建立 CID、Conformer ID 与 ALID 的二级索引。

Compound/Conformer 类型当前根据文件名正则表达式判断；SDF 属性字段名可通过
构造参数扩展。一个 CID 对应多个 Conformer 的关系采用分页 posting list 保存。

### 
### Copyright (c) 2025 by AccelBio.AI, All Rights Reserved. 
********************************************************** '''
from __future__ import annotations

import re
import time
import lmdb
from pathlib import Path
from typing import Optional, Dict, List, Union
from rich.progress import track
from nih.pubchem.index.record_locator import RecordLocator
from nih.pubchem.index.sdf_index import SDFIndex
from nih.pubchem.index.utils_module import (
    _determine_kind,
    _is_int_ascii,
    _iter_sdf_files,
    _make_alid,
    _norm_field_name,
    _uuid_to_keyprefix,
)


# ---------------------------------------------------------------------------
# 默认配置与常量
# ---------------------------------------------------------------------------

# 用于从文件名识别 Compound 数据的默认正则表达式。
DEFAULT_COMPOUND_PATTERNS = [
    r"compound",
    r"cmpd",
    r"compounds",
]

# 用于从文件名识别 Conformer 数据的默认正则表达式。
DEFAULT_CONFORMER_PATTERNS = [
    r"conformer",
    r"conf",
    r"conformers",
]

# SDF 属性块格式为“> <FIELDNAME>”，其后连续非空行是属性值，空行表示结束。
# 以下候选列表用于从不同来源或版本的数据中识别同一业务字段。
# Compound CID 的候选属性名；Compound 标题行是纯数字时也会作为 CID。
CID_FIELD_CANDIDATES = [
    "CID",
    "PUBCHEM_COMPOUND_CID",
    "PUBCHEM_CID",
    "COMPOUND_CID",
]

# Conformer 唯一标识的候选属性名。
CONFORMER_ID_FIELD_CANDIDATES = [
    "CONFORMER_ID",
    "CONFID",
    "PUBCHEM_CONFORMER_ID",
    "CONFORMERID",
]

# Conformer 记录中父 Compound CID 的候选属性名；按尽力而为原则提取。
PARENT_CID_FIELD_CANDIDATES = [
    "CID",
    "PUBCHEM_COMPOUND_CID",
    "PUBCHEM_CID",
    "COMPOUND_CID",
    "PARENT_CID",
]


# CID -> Conformer posting list 每页最多保存的 ALID 数量。
PL_PAGE_SIZE = 4096

# ---------------------------------------------------------------------------
# 索引构建器
# ---------------------------------------------------------------------------


class SDFIndexBuilder:
    """为一个目录树下的 SDF 文件构建 LMDB 偏移索引。

    Args:
        root_dir: 原始 SDF 数据根目录。构建器会递归扫描其中的小写 ``*.sdf``。
        index_dir: LMDB 索引输出目录；不存在时自动创建。
        map_size: LMDB 映射空间上限，单位为字节，默认 1 TiB。
        compound_name_patterns: 用于识别 Compound 文件名的正则表达式字符串
            列表；不传时使用 ``DEFAULT_COMPOUND_PATTERNS``。
        conformer_name_patterns: 用于识别 Conformer 文件名的正则表达式字符串
            列表；不传时使用 ``DEFAULT_CONFORMER_PATTERNS``。
        cid_fields: Compound CID 候选属性名列表。
        conformer_id_fields: Conformer ID 候选属性名列表。
        parent_cid_fields: Conformer 父 CID 候选属性名列表。
        verbose: 是否使用 Rich 显示文件级构建进度。

    Attributes:
        root_dir: 已解析为绝对路径的数据根目录。
        index_dir: 已解析为绝对路径的索引目录。
        idx: 以可写模式打开的 ``SDFIndex`` 实例。

    Notes:
        当前 ``build`` 不负责清空既有 LMDB。完整重建时应使用新的空索引目录，
        避免旧记录或 posting list 与新数据叠加。
    """

    def __init__(
        self,
        root_dir: Union[str, Path],
        index_dir: Union[str, Path],
        map_size: int = 1 << 40,  # 1 TB default
        compound_name_patterns: Optional[List[str]] = None,
        conformer_name_patterns: Optional[List[str]] = None,
        cid_fields: Optional[List[str]] = None,
        conformer_id_fields: Optional[List[str]] = None,
        parent_cid_fields: Optional[List[str]] = None,
        verbose: bool = True,
    ):
        """规范化构建参数、编译匹配模式并打开可写索引。

        参数说明参见类级文档。

        Raises:
            re.error: 文件名匹配模式不是合法正则表达式时抛出。
            OSError: 索引目录无法创建时抛出。
            lmdb.Error: LMDB 环境无法打开时抛出。
        """
        # 第 1 步：把输入和输出目录解析为绝对路径，保证相对路径计算稳定。
        self.root_dir = Path(root_dir).resolve()
        self.index_dir = Path(index_dir).resolve()
        self.map_size = map_size
        self.verbose = verbose

        # 第 2 步：编译文件类型正则表达式，并启用不区分大小写匹配。
        self.compound_patterns = [
            re.compile(p, re.I)
            for p in (compound_name_patterns or DEFAULT_COMPOUND_PATTERNS)
        ]
        self.conformer_patterns = [
            re.compile(p, re.I)
            for p in (conformer_name_patterns or DEFAULT_CONFORMER_PATTERNS)
        ]

        # 第 3 步：统一候选字段名的大小写和首尾空白，降低 SDF 来源差异影响。
        self.cid_fields = [
            _norm_field_name(x) for x in (cid_fields or CID_FIELD_CANDIDATES)
        ]
        self.confid_fields = [
            _norm_field_name(x)
            for x in (conformer_id_fields or CONFORMER_ID_FIELD_CANDIDATES)
        ]
        self.parent_cid_fields = [
            _norm_field_name(x)
            for x in (parent_cid_fields or PARENT_CID_FIELD_CANDIDATES)
        ]

        # 第 4 步：确保输出目录存在，再以可写模式打开全部 LMDB 子数据库。
        self.index_dir.mkdir(parents=True, exist_ok=True)

        self.idx = SDFIndex(self.index_dir, readonly=False, map_size=self.map_size)

    def build(self) -> Dict:
        """扫描全部 SDF 文件、写入索引并返回构建元数据。

        Returns:
            构建元数据字典，包含：

            - ``schema_version``：索引格式版本；
            - ``root_dir``：构建时的数据根目录；
            - ``built_at``：Unix 时间戳；
            - ``pl_page_size``：posting list 每页容量；
            - ``total_files``：已扫描文件数；
            - ``total_records``：完整 SDF 记录总数；
            - ``total_compound_records``：Compound 记录数；
            - ``total_conformer_records``：Conformer 记录数。

        Notes:
            ALID 根据稳定输入确定性生成；本方法不会清理旧索引，也未实现基于
            文件清单的增量更新。
        """
        # 第 1 步：先写入基础元数据，使索引明确记录格式版本和数据来源。
        meta = {
            "schema_version": SDFIndex.SCHEMA_VERSION,
            "root_dir": str(self.root_dir),
            "built_at": int(time.time()),
            "pl_page_size": PL_PAGE_SIZE,
        }
        self.idx._set_meta(meta)

        # 第 2 步：递归收集并排序 SDF 文件，确保跨次构建的处理顺序稳定。
        sdf_files = sorted(_iter_sdf_files(self.root_dir))

        # verbose=True 时使用 Rich 包装迭代器；否则直接遍历文件列表。
        it = (
            track(sdf_files, description="Indexing SDF")
            if self.verbose
            else sdf_files
        )

        # 第 3 步：初始化全局统计计数器。
        total_files = 0
        total_records = 0
        total_compounds = 0
        total_conformers = 0

        # 第 4 步：逐个文件确定相对路径和记录类型，然后流式建立索引。
        for fp in it:
            # LMDB 文件表使用相对路径，并统一成正斜杠以提高跨平台稳定性。
            relpath = str(fp.relative_to(self.root_dir)).replace("\\", "/")
            kind = _determine_kind(fp, self.compound_patterns, self.conformer_patterns)
            stats = self._index_one_file(fp, relpath, kind)

            # 累加当前文件返回的记录分类统计。
            total_files += 1
            total_records += stats["records"]
            total_compounds += stats["compounds"]
            total_conformers += stats["conformers"]

        # 第 5 步：读取基础元数据，补充全局统计后整体写回。
        meta2 = self.idx.get_meta()
        meta2.update(
            {
                "total_files": total_files,
                "total_records": total_records,
                "total_compound_records": total_compounds,
                "total_conformer_records": total_conformers,
            }
        )
        self.idx._set_meta(meta2)

        return meta2

    def _index_one_file(self, fp: Path, relpath: str, kind: str) -> Dict[str, int]:
        """流式扫描单个 SDF 文件并写入该文件的全部索引记录。

        Args:
            fp: 原始 SDF 文件的实际路径。
            relpath: 文件相对于 ``root_dir`` 的稳定路径，使用正斜杠。
            kind: 文件类型，必须遵循 ``"compound"`` 或 ``"conformer"`` 约定；
                非 ``"compound"`` 值会进入 Conformer 分支。

        Returns:
            包含 ``records``、``compounds`` 和 ``conformers`` 三项计数的字典。

        Notes:
            每个文件使用一个 LMDB 写事务。扫描采用二进制模式，``tell`` 得到的
            偏移可直接用于后续 ``seek``。未以 ``$$$$`` 结束的尾部记录会忽略。
        """
        # 第 1 步：初始化当前文件的分类统计。
        records = 0
        compounds = 0
        conformers = 0

        # 第 2 步：为整个文件开启一次写事务，降低逐记录提交的事务开销。
        # 对特别巨大的文件，未来可以扩展为每 N 条记录提交一次。
        with self.idx.env.begin(write=True) as txn:
            # 登记文件路径，已有路径会复用原 file_id。
            file_id = self.idx._get_or_create_file_id(txn, relpath)

            # 第 3 步：以二进制方式打开 SDF，保证字节偏移不受文本解码影响。
            with fp.open("rb") as f:
                # 当前记录的边界状态：起点和文件内从 0 开始的记录序号。
                rec_start = f.tell()
                rec_no = 0

                # SDF 属性解析状态：标题行、是否位于属性值、当前字段及其值行。
                title_line: Optional[bytes] = None
                in_prop = False
                cur_field: Optional[str] = None
                cur_val_lines: List[bytes] = []

                # 从当前记录提取的业务键；每完成一条记录后全部重置。
                cid_val: Optional[int] = None
                conf_id_val: Optional[str] = None
                parent_cid_val: Optional[int] = None

                def finalize_field():
                    """结束当前 SDF 属性，并把首个非空值写入对应业务变量。

                    本地函数没有入口参数和返回值，通过 ``nonlocal`` 更新当前
                    记录的 CID、Conformer ID、父 CID 以及属性解析状态。无法识别
                    的属性会被忽略；数值 CID 仅接受纯 ASCII 数字。
                    """
                    nonlocal cur_field, cur_val_lines, cid_val, conf_id_val, parent_cid_val

                    # 没有活动字段时无需解析，常见于普通结构行或连续空行。
                    if not cur_field:
                        return

                    # 第 1 步：按 SDF 常见约定，取属性值中的首个非空行。
                    v = None
                    for line in cur_val_lines:
                        s = line.strip()
                        if s:
                            v = s
                            break

                    # 第 2 步：属性值存在时规范化字段名，并按候选列表提取业务键。
                    if v is not None:
                        fn = _norm_field_name(cur_field)

                        # 每类字段只记录首次成功解析的值，避免后续同名属性覆盖。
                        if fn in self.cid_fields and cid_val is None:
                            if _is_int_ascii(v):
                                cid_val = int(v.decode("ascii"))
                        if fn in self.parent_cid_fields and parent_cid_val is None:
                            if _is_int_ascii(v):
                                parent_cid_val = int(v.decode("ascii"))
                        if fn in self.confid_fields and conf_id_val is None:
                            # Conformer ID 可能是数字或文本，统一按 UTF-8 字符串保存。
                            conf_id_val = v.decode("utf-8", errors="replace")

                    # 第 3 步：清空字段状态，为后续属性块做准备。
                    cur_field = None
                    cur_val_lines = []

                # 第 4 步：逐行运行 SDF 解析状态机，直到文件结束。
                while True:
                    line = f.readline()
                    if not line:
                        # 到达 EOF；没有 $$$$ 终止符的残缺尾记录不会写入索引。
                        break

                    # 记录第一行是标题。Compound 数据常直接用纯数字 CID 作标题。
                    if title_line is None:
                        title_line = line.rstrip(b"\r\n")
                        # Compound 文件的标题行通常就是 CID，可直接尝试提取。
                        if kind == "compound":
                            t = title_line.strip()
                            if _is_int_ascii(t):
                                cid_val = int(t.decode("ascii"))

                    # 识别形如“> <FIELDNAME>”的 SDF 属性头。
                    if line.startswith(b"> <") and line.rstrip().endswith(b">"):
                        # 新属性开始前，先提交上一个仍处于活动状态的属性。
                        finalize_field()

                        # 提取尖括号中的字段名；异常格式按非属性行处理。
                        try:
                            # 示例：b"> <PUBCHEM_COMPOUND_CID>\n"
                            m = re.search(rb"> <([^>]+)>", line)
                            if m:
                                cur_field = m.group(1).decode("utf-8", errors="replace")
                                cur_val_lines = []
                                in_prop = True
                            else:
                                cur_field = None
                                in_prop = False
                        except Exception:
                            cur_field = None
                            in_prop = False
                        # 属性头本身不应再进入属性值或记录终止符处理。
                        continue

                    # 属性值持续到空行；非空行暂存，结束时由 finalize_field 解析。
                    if in_prop:
                        if line.strip() == b"":
                            finalize_field()
                            in_prop = False
                        else:
                            cur_val_lines.append(line)
                        # 这里保留向下执行，以便同一行仍可参与记录终止符判断。
                        pass

                    # “$$$$”表示一条完整 SDF 记录结束。
                    if line.strip() == b"$$$$":
                        # 第 5 步：提交最后一个属性，并记录终止符后的字节位置。
                        finalize_field()
                        rec_end = f.tell()

                        # 第 6 步：根据文件类型确定 ALID 主标识、记录标志和有效 CID。
                        if kind == "compound":
                            primary = str(cid_val) if cid_val is not None else ""
                            alid = _make_alid("compound", relpath, rec_no, primary)
                            is_conf = False
                            eff_cid = cid_val
                        else:
                            primary = conf_id_val or ""
                            alid = _make_alid("conformer", relpath, rec_no, primary)
                            is_conf = True
                            # Conformer 优先使用通用 CID，缺失时再使用父 CID 字段。
                            eff_cid = cid_val if cid_val is not None else parent_cid_val

                        # 第 7 步：构造“类型前缀 + UUID16”主键及固定长度定位值。
                        rec_key = _uuid_to_keyprefix(is_conf) + alid.bytes
                        loc = RecordLocator(
                            file_id=file_id,
                            start=rec_start,
                            end=rec_end,
                            is_conformer=is_conf,
                            cid=eff_cid,
                        )
                        txn.put(rec_key, loc.to_bytes(), db=self.idx.db_records)

                        # 第 8 步：根据记录类型建立对应二级索引并更新分类计数。
                        if not is_conf:
                            # Compound 使用唯一的“CID -> 主记录键”映射。
                            if cid_val is not None:
                                txn.put(
                                    str(int(cid_val)).encode("ascii"),
                                    rec_key,
                                    db=self.idx.db_cid_to_compound,
                                )
                            compounds += 1
                        else:
                            # Conformer 使用唯一的“Conformer ID -> 主记录键”映射。
                            if conf_id_val is not None:
                                txn.put(
                                    conf_id_val.encode("utf-8"),
                                    rec_key,
                                    db=self.idx.db_confid_to_conf,
                                )

                            # 同时把 ALID 追加到父 CID 的一对多分页 posting list。
                            if eff_cid is not None:
                                self._pl_append(txn, eff_cid, alid.bytes)
                            conformers += 1

                        records += 1
                        rec_no += 1

                        # 第 9 步：终止符后的当前位置即下一条记录起点，并清空状态。
                        rec_start = f.tell()
                        title_line = None
                        in_prop = False
                        cur_field = None
                        cur_val_lines = []
                        cid_val = None
                        conf_id_val = None
                        parent_cid_val = None

        # 文件事务成功提交后返回该文件的统计信息。
        return {"records": records, "compounds": compounds, "conformers": conformers}

    def _pl_append(self, txn: lmdb.Transaction, cid: int, uuid16: bytes) -> None:
        """把一个 Conformer ALID 追加到 CID 的分页 posting list。

        Args:
            txn: 已由调用方开启的 LMDB 写事务。
            cid: Conformer 所属的父 Compound CID。
            uuid16: Conformer ALID 的 16 字节 UUID 原始值。

        Returns:
            无返回值。页头和页内容直接写入当前事务。

        Notes:
            头表保存页数；页表键格式为 ``CID|page_no``，值为连续拼接的 UUID16。
            每页最多 ``PL_PAGE_SIZE`` 条，满页后创建下一页。
        """
        # 第 1 步：规范化 CID 键，并读取当前 posting list 的页数。
        cid_k = str(int(cid)).encode("ascii")
        h = txn.get(cid_k, db=self.idx.db_cid2conf_h)
        page_count = int.from_bytes(h, "little") if h else 0

        # 第 2 步：没有任何页面时创建第 0 页，并把头表页数设置为 1。
        if page_count == 0:
            page_no = 0
            pk = cid_k + b"|" + b"0"
            txn.put(pk, uuid16, db=self.idx.db_cid2conf_p)
            txn.put(cid_k, (1).to_bytes(4, "little"), db=self.idx.db_cid2conf_h)
            return

        # 第 3 步：读取最后一页，并根据字节长度计算已有 UUID 数量。
        last_page_no = page_count - 1
        pk = cid_k + b"|" + str(last_page_no).encode("ascii")
        blob = txn.get(pk, db=self.idx.db_cid2conf_p) or b""
        n = len(blob) // 16

        # 最后一页未满时，直接把新的 UUID16 追加到页尾。
        if n < PL_PAGE_SIZE:
            txn.put(pk, blob + uuid16, db=self.idx.db_cid2conf_p)
            return

        # 第 4 步：最后一页已满时创建新页，并同步递增头表中的总页数。
        new_page_no = page_count
        pk2 = cid_k + b"|" + str(new_page_no).encode("ascii")
        txn.put(pk2, uuid16, db=self.idx.db_cid2conf_p)
        txn.put(
            cid_k, (page_count + 1).to_bytes(4, "little"), db=self.idx.db_cid2conf_h
        )
