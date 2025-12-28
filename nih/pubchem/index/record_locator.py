# -*- coding: utf-8 -*-
"""************************************************************
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 20:54:46
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 21:05:17
### FilePath: //pubchem//nih//pubchem//index//record_locator.py
### Description:
###
### Copyright (c) 2025 by AI Lingues, All Rights Reserved.
**********************************************************"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Optional

# LMDB value packing for a record locator (fixed-size, fast decode):
# file_id: uint32
# start:  uint64
# end:    uint64
# flags:  uint16  (bit0: is_conformer)
# cid:    int64   (-1 if missing)
# reserved: uint16
# Total: 4 + 8 + 8 + 2 + 8 + 2 = 32 bytes
RECORD_STRUCT = struct.Struct("<IQQHqH")

FLAG_IS_CONFORMER = 0x0001

# -----------------------------
# Data models
# -----------------------------


@dataclass(frozen=True)
class RecordLocator:
    """Pointer to a record segment in a specific file."""

    file_id: int
    start: int
    end: int
    is_conformer: bool
    cid: Optional[int] = None  # parent/compound CID (for conformer too, if available)

    def to_bytes(self) -> bytes:
        cid_i64 = -1 if self.cid is None else int(self.cid)
        flags = FLAG_IS_CONFORMER if self.is_conformer else 0
        return RECORD_STRUCT.pack(
            int(self.file_id), int(self.start), int(self.end), flags, cid_i64, 0
        )

    @staticmethod
    def from_bytes(b: bytes) -> "RecordLocator":
        file_id, start, end, flags, cid_i64, _ = RECORD_STRUCT.unpack(b)
        cid = None if cid_i64 == -1 else int(cid_i64)
        return RecordLocator(
            file_id=int(file_id),
            start=int(start),
            end=int(end),
            is_conformer=bool(flags & FLAG_IS_CONFORMER),
            cid=cid,
        )
