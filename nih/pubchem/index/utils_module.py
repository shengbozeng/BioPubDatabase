# -*- coding: utf-8 -*-
"""************************************************************
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 20:55:40
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 21:05:05
### FilePath: //pubchem//nih//pubchem//index//utils.py
### Description:
###
### Copyright (c) 2025 by AI Lingues, All Rights Reserved.
**********************************************************"""

from __future__ import annotations

import re
import uuid
import hashlib
from pathlib import Path
from typing import Iterable, Iterator, List

# UUID namespace for deterministic ALID
ALID_NAMESPACE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")  # UUID namespace URL


# -----------------------------
# Utility
# -----------------------------


def _norm_field_name(name: str) -> str:
    return name.strip().upper()


def _is_int_ascii(s: bytes) -> bool:
    # allow leading/trailing spaces already stripped before calling
    return s.isdigit()


def _sha1_bytes(data: bytes) -> bytes:
    return hashlib.sha1(data).digest()


def _iter_sdf_files(root_dir: Path) -> Iterator[Path]:
    for p in root_dir.rglob("*.sdf"):
        if p.is_file():
            yield p


def _match_any(patterns: List[re.Pattern], text: str) -> bool:
    t = text.lower()
    return any(p.search(t) for p in patterns)

# todo 当前探测方式是通过文件名判断，须改为通过文件内容中是否包含PUBCHEM_CONFORMER_ID属性进行判断
def _determine_kind(
    file_path: Path,
    compound_patterns: List[re.Pattern],
    conformer_patterns: List[re.Pattern],
) -> str:
    name = file_path.name.lower()
    # Prefer explicit conformer match if both match
    if _match_any(conformer_patterns, name):
        return "conformer"
    if _match_any(compound_patterns, name):
        return "compound"
    # Fallback heuristic: if contains "conf" anywhere, treat as conformer
    if "conf" in name:
        return "conformer"
    return "compound"


def _uuid_to_keyprefix(is_conformer: bool) -> bytes:
    return b"F" if is_conformer else b"C"


def _make_alid(kind: str, relpath: str, rec_no: int, primary_id: str) -> uuid.UUID:
    """
    Deterministic ALID:
    - kind: "compound" or "conformer"
    - relpath: relative file path inside indexed root
    - rec_no: record order in file
    - primary_id: CID or conformer_id if available else ""
    """
    # Keep string stable; avoid absolute paths.
    s = f"{kind}|{relpath}|{rec_no}|{primary_id}"
    return uuid.uuid5(ALID_NAMESPACE, s)


def _chunked(iterable: Iterable, chunk_size: int) -> Iterator[List]:
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= chunk_size:
            yield buf
            buf = []
    if buf:
        yield buf
