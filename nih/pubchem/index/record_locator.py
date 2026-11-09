# -*- coding: utf-8 -*-
"""************************************************************
### Author: Zeng Shengbo shengbo.zeng@accelbio.ai
### Date: 12/27/2025 20:54:46
### LastEditors: Zeng Shengbo shengbo.zeng@accelbio.ai
### LastEditTime: 11/9/2026 10:15:05
### FilePath: //pubchem//nih//pubchem//index//record_locator.py
### Description:
# 定义 SDF 原始记录的二进制定位信息。

索引不复制完整的 SDF 文本，而是为每条记录保存所属文件、起止字节偏移、记录
类型和关联 CID。本模块提供不可变的 ``RecordLocator`` 数据模型，以及它与
LMDB 固定长度二进制值之间的序列化和反序列化逻辑。

二进制结构固定为 32 字节，字段采用小端序存储，便于快速读取并减少索引体积。

###
### Copyright (c) 2025 by AccelBio.AI, All Rights Reserved.
**********************************************************"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Optional

# LMDB 中记录定位值的二进制布局（小端序、固定长度，便于常量时间解码）：
# file_id: uint32，原始文件在索引文件表中的编号；
# start: uint64，SDF 记录起始字节偏移（包含）；
# end: uint64，SDF 记录结束字节偏移（不包含）；
# flags: uint16，标志位，最低位表示是否为 Conformer；
# cid: int64，Compound CID 或 Conformer 的父 CID，-1 表示缺失；
# reserved: uint16，保留字段，当前固定写入 0。
# 总长度：4 + 8 + 8 + 2 + 8 + 2 = 32 字节。
RECORD_STRUCT = struct.Struct("<IQQHqH")

# flags 字段最低位：置 1 表示 Conformer，置 0 表示 Compound。
FLAG_IS_CONFORMER = 0x0001

# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecordLocator:
    """一条 SDF 记录在原始文件中的定位信息。

    Attributes:
        file_id: 原始 SDF 文件在 LMDB 文件表中的内部编号。
        start: 记录起始字节偏移，读取时包含该位置。
        end: 记录结束字节偏移，读取时不包含该位置。
        is_conformer: ``True`` 表示 Conformer，``False`` 表示 Compound。
        cid: Compound CID；对于 Conformer，表示其父 Compound CID。无法提取时
            为 ``None``。

    ``frozen=True`` 使实例不可变，避免索引命中后定位信息被意外修改。
    """

    file_id: int
    start: int
    end: int
    is_conformer: bool
    cid: Optional[int] = None  # Compound CID；Conformer 有值时保存其父 CID。

    def to_bytes(self) -> bytes:
        """将定位信息序列化为 LMDB 使用的 32 字节值。

        Returns:
            严格符合 ``RECORD_STRUCT`` 布局的二进制数据。

        Notes:
            ``None`` CID 使用 -1 作为哨兵值；记录类型通过 flags 最低位保存；
            保留字段当前固定为 0。
        """
        # 第 1 步：将可空 CID 映射为二进制结构可存储的有符号整数。
        cid_i64 = -1 if self.cid is None else int(self.cid)

        # 第 2 步：根据记录类型设置 flags 的 Conformer 标志位。
        flags = FLAG_IS_CONFORMER if self.is_conformer else 0

        # 第 3 步：将所有字段按固定顺序、小端序打包为 32 字节。
        return RECORD_STRUCT.pack(
            int(self.file_id), int(self.start), int(self.end), flags, cid_i64, 0
        )

    @staticmethod
    def from_bytes(b: bytes) -> "RecordLocator":
        """从 LMDB 二进制值恢复 ``RecordLocator``。

        Args:
            b: 由 ``to_bytes`` 生成的 32 字节二进制数据。

        Returns:
            解码后的不可变记录定位对象。

        Raises:
            struct.error: 输入长度不是 32 字节或不符合既定结构时抛出。
        """
        # 第 1 步：按固定结构一次性解包各字段；保留字段不参与业务逻辑。
        file_id, start, end, flags, cid_i64, _ = RECORD_STRUCT.unpack(b)

        # 第 2 步：把序列化时使用的 -1 哨兵值还原为 None。
        cid = None if cid_i64 == -1 else int(cid_i64)

        # 第 3 步：通过按位与判断记录类型，并构造强类型结果对象。
        return RecordLocator(
            file_id=int(file_id),
            start=int(start),
            end=int(end),
            is_conformer=bool(flags & FLAG_IS_CONFORMER),
            cid=cid,
        )
