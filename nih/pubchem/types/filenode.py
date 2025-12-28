# -*- coding: utf-8 -*-
''' ************************************************************ 
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 16:07:24
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 19:31:19
### FilePath: //pubchem//nih//pubchem//types//filenode.py
### Description: 
### 
### Copyright (c) 2025 by AI Lingues, All Rights Reserved. 
********************************************************** '''
from enum import Enum

class ENihPubChemDataType(Enum):
    Compound='Compound'
    Conformer='Conformer'
    Unkown="Unknow"

# 1. 数据结构 (保持不变)
class FileNode:
    def __init__(self, file_name, href, last_modified, size):
        self.file_name :str = file_name
        self.href :str = href
        self.last_modified = last_modified
        self.size = size


