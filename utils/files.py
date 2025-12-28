from typing import List
from pathlib import Path

def get_files_by_extension(
    directory: str, extension: str, exclude_name: str = None
) -> List[str]:
    """
    检索指定目录及其子目录下特定扩展名的文件

    :param directory: 目标目录路径
    :param extension: 扩展名 (如 ".txt" 或 "txt")
    :param exclude_name: 要排除的文件全名 (包含扩展名，如 "config.json")
    :return: 绝对路径字符串列表
    """
    # 确保扩展名以 . 开头
    ext = f".{extension.lstrip('.')}"

    root_path = Path(directory)
    if not root_path.is_dir():
        print(f"错误: 路径 {directory} 不是有效的目录")
        return []

    # rglob 代表 recursive glob，即递归搜索
    # * 表示匹配文件名，{ext} 匹配后缀
    files = []
    for p in root_path.rglob(f"*{ext}"):
        # p 是一个 Path 对象，p.name 是带后缀的文件名
        if exclude_name and p.name == exclude_name:
            continue

        # 将 Path 对象转换为绝对路径字符串
        files.append(str(p.absolute()))

    return files
