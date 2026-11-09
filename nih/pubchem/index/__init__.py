"""PubChem SDF 偏移索引包的公共接口。

本包负责将大型 Compound/Conformer SDF 文件构建为 LMDB 偏移索引，并提供
按 ALID、CID、Conformer ID 查询原始记录的能力。外部代码通常只需从本模块
导入索引构建器、索引读取器及其返回的数据模型，无需依赖内部模块路径。

公开对象：
    SDFIndexBuilder: 扫描 SDF 文件并构建 LMDB 索引。
    SDFIndex: 打开并查询已经构建的索引。
    IndexHit: 索引命中结果，包含 ALID 和记录定位信息。
    RecordLocator: 原始 SDF 记录在文件中的字节区间描述。
    get_compound: 按 CID 列表批量读取 Compound SDF 文本。
    get_conformer: 按 Conformer ID 列表批量读取 Conformer SDF 文本。
"""

from nih.pubchem.index.get_compound import get_compound, get_conformer
from nih.pubchem.index.record_locator import RecordLocator
from nih.pubchem.index.sdf_index import IndexHit, SDFIndex
from nih.pubchem.index.sdf_index_builder import SDFIndexBuilder

__all__ = [
    "IndexHit",
    "RecordLocator",
    "SDFIndex",
    "SDFIndexBuilder",
    "get_compound",
    "get_conformer",
]
