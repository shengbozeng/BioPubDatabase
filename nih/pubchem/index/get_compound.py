"""从本地 LMDB 偏移索引中批量提取 PubChem SDF 记录。

模块提供 Compound 与 Conformer 两类批量读取函数。函数先通过二级索引取得
``RecordLocator``，再到原始 SDF 根目录按字节区间读取记录；模块底部还保留了
一个完整示例，用于获取 Compound、查询其在线 Conformer 关联并分别输出 SDF。
"""

import json
from pathlib import Path
from typing import Dict, List
from pybiotech.loaders.nih.pubchem.online.conformer import get_compound_conformer_ids
from nih.pubchem.index.sdf_index import SDFIndex


def get_compound(
        cid_list: List[str | int],
        index_path: str,
        root_path: str,
) -> Dict[str, str]:
    """按 CID 列表批量读取 Compound 的原始 SDF 文本。

    Args:
        cid_list: 待查询的 CID 列表。元素可以是整数或能转换为整数的字符串。
        index_path: Compound LMDB 索引目录路径。
        root_path: 建立索引时所对应的原始 SDF 根目录。

    Returns:
        以 CID 字符串为键、完整 SDF 记录文本为值的字典。未命中的 CID 不会
        出现在返回字典中，仅会在标准输出打印 ``NOT FOUND``。

    Raises:
        ValueError: ``cid_list`` 中存在无法转换为整数的值时抛出。
        KeyError: 索引命中记录所引用的文件编号无法解析时抛出。
        OSError: 索引目录或原始 SDF 文件无法打开时抛出。
    """
    # 第 1 步：以只读模式打开索引，避免查询过程修改 LMDB 数据。
    idx = SDFIndex(index_path, readonly=True)

    # 第 2 步：统一把外部传入的 CID 转换成整数，匹配索引键的规范形式。
    cids: List[int] = [int(cid) for cid in cid_list]
    sdf_content_dict: Dict[str, str] = {}

    # 第 3 步：逐个查询 CID，并根据命中的字节偏移读取原始 SDF 片段。
    for id, cid in enumerate(cids):
        hit = idx.get_compound_by_cid(cid)
        if not hit:
            print("NOT FOUND")
        else:
            seg_compound = idx.read_segment(root_path, hit.locator)
            # SDF 通常为 UTF-8/ASCII 文本；异常字节使用替换字符保留整体记录。
            sdf_content_dict[str(hit.locator.cid)]=seg_compound.decode("utf-8", errors="replace")

    # 第 4 步：返回所有成功读取的 Compound 记录。
    return sdf_content_dict



def get_conformer(index_path: str, root_path: str, confid_list: List[str]) -> None | dict[str, str | None]:
    """按 Conformer ID 列表批量读取原始 Conformer SDF 文本。

    Args:
        index_path: Conformer LMDB 索引目录路径。
        root_path: 建立索引时所对应的原始 Conformer SDF 根目录。
        confid_list: 待查询的 Conformer ID 字符串列表，内容必须与索引中的
            ID 完全一致。

    Returns:
        字典的键为输入的 Conformer ID；命中时值为 SDF 文本，未命中时值为
        ``None``。当前实现总是返回字典，返回类型中的 ``None`` 为兼容性声明。

    Raises:
        KeyError: 索引命中记录所引用的文件编号无法解析时抛出。
        OSError: 索引目录或原始 SDF 文件无法打开时抛出。
    """

    # 第 1 步：以只读模式打开 Conformer 索引，并初始化结果字典。
    idx = SDFIndex(index_path, readonly=True)
    conformer_dict: dict[str, str | None] = {}

    # 第 2 步：逐个查询 Conformer ID。
    for confid in confid_list:
        hit = idx.get_conformer_by_conformer_id(confid)
        if not hit:
            # 保留未命中的输入键，使调用方可以区分“未找到”和“未查询”。
            print("NOT FOUND")
            conformer_dict[confid] = None
        else:
            # 第 3 步：按定位信息读取原始记录，并以容错方式解码为文本。
            seg = idx.read_segment(root_path, hit.locator)
            conformer_dict[confid] = seg.decode("utf-8", errors="replace")

    return conformer_dict


# ---------------------------------------------------------------------------
# 示例入口：演示 Compound 查询、在线获取关联 Conformer ID 以及 SDF 文件输出。
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # 第 1 步：设置 Compound/Conformer 的索引目录与原始数据根目录。
    cid_list: List[str] = []

    compound_index_path = '/ai/data/pubchem_unzipped_data/compound'
    compound_root_path = '/ai/data/pubchem_unzipped_data/compound'
    
    conformer_index_path = '/ai/data/pubchem_unzipped_data/conformer'
    conformer_root_path = '/ai/data/pubchem_unzipped_data/conformer'

    # 第 2 步：从文本文件读取 CID，并清理每行首尾空白。
    with open('test/nih/pubchem/index/cid_list.txt', 'r', encoding='utf-8') as f:
        cid_list.extend(f.readlines())
    cid_list = [str(cid).strip() for cid in cid_list]

    # 第 3 步：查询 Compound 记录，并顺序写入一个聚合 SDF 文件。
    sdf_content_dict = get_compound(cid_list, compound_index_path, compound_root_path)
    with open('test/nih/pubchem/index/test_compound.sdf', 'w', encoding='utf-8') as f:
        i=0
        for key,value in sdf_content_dict.items():
            f.write(value)
            i+=1
        f.flush()
        print(f"compound count:\t{i}")

    # 第 4 步：优先复用本地缓存；缓存不存在时从在线接口查询 CID 与
    # Conformer ID 的关联关系，并将结果保存为 JSON。
    data_json = Path(__file__).resolve().parent / 'data.json'
    if not data_json.exists():
        data=get_compound_conformer_ids(cid_list,ignore_error=True)        
        with open(data_json, 'w', encoding='utf-8') as f:
            # indent=4 让生成的 JSON 有层级缩进，易于阅读
            # ensure_ascii=False 保证中文字符能正常显示而非 \uXXXX
            json.dump(data, f, indent=4, ensure_ascii=False)
    else:
        # 缓存已经存在时直接读取，避免重复访问在线接口。
        with open(data_json, 'r', encoding='utf-8') as f:
            data = json.load(f)

    # 第 5 步：将各 CID 对应的 Conformer ID 列表展平为一个查询列表。
    confid_list = []
    for s in list(data.values()):
        if len(s)>0:
            confid_list.extend(s)

    # 第 6 步：从本地 Conformer 索引读取所有关联记录。
    conformer_data = get_conformer(index_path=conformer_index_path,root_path=conformer_root_path,confid_list=confid_list)

    # 第 7 步：跳过未命中或空记录，将有效 Conformer 顺序写入 SDF 文件。
    with open('test/nih/pubchem/index/test_conformer.sdf', 'w', encoding='utf-8') as f:
        i=0
        for key,value in conformer_data.items():  
            if value is None or len(value)==0:
                print(key)   
                continue       
            f.write(value)
            i+=1
        f.flush()
        print(f"conformer count:\t{i}")
    print('done.')
