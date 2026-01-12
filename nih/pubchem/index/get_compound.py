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
    idx = SDFIndex(index_path, readonly=True)
    cids: List[int] = [int(cid) for cid in cid_list]
    sdf_content_dict: Dict[str, str] = {}
    for id, cid in enumerate(cids):
        hit = idx.get_compound_by_cid(cid)
        if not hit:
            print("NOT FOUND")
        else:
            seg_compound = idx.read_segment(root_path, hit.locator)
            sdf_content_dict[str(hit.locator.cid)]=seg_compound.decode("utf-8", errors="replace")
    
    data_json = Path(__file__).resolve().parent / 'data.json'
    if not data_json.exists():
        data=get_compound_conformer_ids(cid_list,ignore_error=True)        
        # 写入文件
        with open(data_json, 'w', encoding='utf-8') as f:
            # indent=4 让生成的 JSON 有层级缩进，易于阅读
            # ensure_ascii=False 保证中文字符能正常显示而非 \uXXXX
            json.dump(data, f, indent=4, ensure_ascii=False)
    else:
        # 读取文件
        with open(data_json, 'r', encoding='utf-8') as f:
            # 将 JSON 文件内容转换为 Python 字典
            data = json.load(f)
    
    confid_list = []
    for s in list(data.values()):
        if len(s)>0:
            confid_list.append(s[0])
    seg_conformer = get_conformer(idx, root_path, confid_list)
    
   
    return sdf_content_dict



def get_conformer(index: SDFIndex, root_path: str, confid_list: List[str]) -> None | str:

    idx = index
    conformer_dict={}
    for confid in confid_list:
        hit = idx.get_conformer_by_conformer_id(confid)
        if not hit:
            print("NOT FOUND")
            conformer_dict[confid] = None
        else:
            seg = idx.read_segment(root_path, hit.locator)
            conformer_dict[confid] = seg.decode("utf-8", errors="replace")

    return conformer_dict


# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    cid_list: List[str] = []

    index_path = '/projects/projects/pubchem/nih/pubchem/index/test_index'
    root_path = '/projects/projects/pubchem/nih/pubchem/index/test_index'

    with open('test/nih/pubchem/index/cid_list.txt', 'r', encoding='utf-8') as f:
        cid_list.extend(f.readlines())
    cid_list = [str(cid).strip() for cid in cid_list]
    sdf_content_dict = get_compound(cid_list, index_path, root_path)
    with open('test/nih/pubchem/index/test_compound.sdf', 'w', encoding='utf-8') as f:
        i=0
        for key,value in sdf_content_dict.items():
            f.write(value.get('compound'))
            i+=1
        f.flush()
        print(f"compound count:\t{i}")
    
    with open('test/nih/pubchem/index/test_conformer.sdf', 'w', encoding='utf-8') as f:
        i=0
        for key,value in sdf_content_dict.items():  
            if len(value.get('conformer'))==0:
                print(key)          
            f.write(value.get('conformer'))
            i+=1
        f.flush()
        print(f"conformer count:\t{i}")
    print('done.')
