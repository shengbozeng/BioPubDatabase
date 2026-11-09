# -*- coding: utf-8 -*-
"""************************************************************
### Author: Zeng Shengbo shengbo.zeng@accelbio.ai
### Date: 12/27/2025 20:55:40
### LastEditors: Zeng Shengbo shengbo.zeng@accelbio.ai
### LastEditTime: 11/9/2026 10:15:05
### FilePath: //pubchem//nih//pubchem//index//utils.py
### Description:
SDF 索引构建和批量查询使用的内部辅助函数。

本模块集中处理字段名规范化、整数识别、SDF 文件遍历、文件类型判断、确定性
ALID 生成和可迭代对象分块。以下函数以下划线开头，表示它们属于索引实现细节，
外部调用方通常应使用 ``SDFIndex`` 或 ``SDFIndexBuilder`` 的公开接口。

###
### Copyright (c) 2025 by AccelBio.AI, All Rights Reserved.
**********************************************************"""

from __future__ import annotations

import re
import uuid
import hashlib
from pathlib import Path
from typing import Iterable, Iterator, List

# UUIDv5 的固定命名空间。固定命名空间配合稳定输入可保证 ALID 可重复生成。
ALID_NAMESPACE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")  # URL 命名空间 UUID


# ---------------------------------------------------------------------------
# 通用内部工具
# ---------------------------------------------------------------------------


def _norm_field_name(name: str) -> str:
    """规范化 SDF 属性名，便于进行不区分大小写的候选字段匹配。

    Args:
        name: 原始属性名，可能包含首尾空白或大小写差异。

    Returns:
        去除首尾空白并转换为大写的属性名。
    """
    return name.strip().upper()


def _is_int_ascii(s: bytes) -> bool:
    """判断字节串是否只包含 ASCII 数字。

    Args:
        s: 已由调用方去除首尾空白的属性值字节串。

    Returns:
        全部字符均为十进制数字时返回 ``True``，否则返回 ``False``。

    Notes:
        本函数不接受正负号、小数点或内部空格，适用于非负 CID 判断。
    """
    # 调用前应已经清除首尾空白，这里只检查剩余内容是否全部为数字。
    return s.isdigit()


def _sha1_bytes(data: bytes) -> bytes:
    """计算输入数据的 SHA-1 原始摘要。

    Args:
        data: 待计算摘要的二进制数据。

    Returns:
        长度为 20 字节的 SHA-1 二进制摘要，而不是十六进制字符串。
    """
    return hashlib.sha1(data).digest()


def _iter_sdf_files(root_dir: Path) -> Iterator[Path]:
    """递归遍历根目录下的所有小写 ``.sdf`` 文件。

    Args:
        root_dir: 原始 SDF 数据根目录。

    Yields:
        每个实际存在的 ``.sdf`` 文件路径。目录和其他扩展名会被跳过。
    """
    # rglob 负责递归搜索；逐个 yield 可避免额外构建完整结果列表。
    for p in root_dir.rglob("*.sdf"):
        if p.is_file():
            yield p


def _match_any(patterns: List[re.Pattern], text: str) -> bool:
    """判断文本是否命中任意一个已编译正则表达式。

    Args:
        patterns: 已编译的正则表达式列表。
        text: 待匹配文本。

    Returns:
        至少一个表达式匹配时返回 ``True``。
    """
    # 先统一为小写；默认模式本身还会以 re.I 编译，以增强调用兼容性。
    t = text.lower()
    return any(p.search(t) for p in patterns)

# TODO：当前类型探测依赖文件名，后续可改为检查记录中是否包含
# PUBCHEM_CONFORMER_ID 属性，以适配名称中不带 conf/conformer 的官方文件。
def _determine_kind(
    file_path: Path,
    compound_patterns: List[re.Pattern],
    conformer_patterns: List[re.Pattern],
) -> str:
    """根据文件名判断 SDF 文件保存的是 Compound 还是 Conformer。

    Args:
        file_path: 待分类的 SDF 文件路径；只使用其文件名参与匹配。
        compound_patterns: Compound 文件名正则表达式列表。
        conformer_patterns: Conformer 文件名正则表达式列表。

    Returns:
        ``"conformer"`` 或 ``"compound"``。若没有任何模式命中，则默认返回
        ``"compound"``。

    Notes:
        Conformer 模式拥有更高优先级；当两组模式同时命中时仍判定为
        Conformer。
    """
    # 第 1 步：只取文件名并转为小写，避免父目录名称影响类型判断。
    name = file_path.name.lower()

    # 第 2 步：优先匹配 Conformer，处理两类模式同时命中的情况。
    if _match_any(conformer_patterns, name):
        return "conformer"

    # 第 3 步：未命中 Conformer 时再判断显式 Compound 模式。
    if _match_any(compound_patterns, name):
        return "compound"

    # 第 4 步：使用“conf”子串作为 Conformer 的兜底启发式规则。
    if "conf" in name:
        return "conformer"

    # 无法识别时按 Compound 处理，这是当前实现的默认约定。
    return "compound"


def _uuid_to_keyprefix(is_conformer: bool) -> bytes:
    """返回 LMDB 主记录键使用的单字节类型前缀。

    Args:
        is_conformer: 是否为 Conformer 记录。

    Returns:
        Conformer 返回 ``b"F"``，Compound 返回 ``b"C"``。
    """
    return b"F" if is_conformer else b"C"


def _make_alid(kind: str, relpath: str, rec_no: int, primary_id: str) -> uuid.UUID:
    """根据记录的稳定属性生成确定性 ALID（UUIDv5）。

    Args:
        kind: 记录类型，当前约定为 ``"compound"`` 或 ``"conformer"``。
        relpath: 原始 SDF 文件相对于索引根目录的路径。
        rec_no: 记录在所属文件内从 0 开始的顺序编号。
        primary_id: Compound CID 或 Conformer ID；无法提取时传空字符串。

    Returns:
        基于固定命名空间生成的 UUIDv5。只要四个输入不变，结果就保持不变。
    """
    # 使用明确分隔符组成稳定名称，并避免把机器相关的绝对路径写入标识。
    s = f"{kind}|{relpath}|{rec_no}|{primary_id}"
    return uuid.uuid5(ALID_NAMESPACE, s)


def _chunked(iterable: Iterable, chunk_size: int) -> Iterator[List]:
    """把任意可迭代对象按指定大小分批输出。

    Args:
        iterable: 输入可迭代对象，可为列表、生成器或其他迭代器。
        chunk_size: 每批最多包含的元素数量。调用方应传入正整数。

    Yields:
        按输入顺序组成的列表；最后一批可能少于 ``chunk_size``。
    """
    # 缓冲区只保存当前批次，避免批量查询时一次性物化全部输入。
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= chunk_size:
            yield buf
            buf = []

    # 迭代结束后输出不足一个完整批次的剩余元素。
    if buf:
        yield buf
