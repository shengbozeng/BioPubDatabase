"""批量解压并校验 PubChem 压缩数据的脚本入口。

脚本会递归查找 ``input_dir`` 下的 ``.gz`` 文件，通过 ``ArchiveIO`` 解压到
同一数据目录；如果压缩文件旁存在同名的 ``.md5`` 文件，则同时校验压缩包的
MD5，并记录校验失败的文件路径。

本模块当前使用代码内固定目录，主要用于数据准备阶段的人工执行，不提供命令行
参数。直接运行方式为 ``python -m nih.pubchem.index.create``。
"""

from pathlib import Path
from ailingues_core.utils.archive_io import ArchiveIO, ArchiveType

from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from utils.files import get_files_by_extension
from utils.md5_check import verify_md5


if __name__=="__main__":
    # 第 1 步：确定待处理目录，并递归收集其中所有 gzip 压缩文件。
    input_dir = Path('./nih/pubchem/index/test_index')
    sdf_files=get_files_by_extension(str(input_dir),extension='.gz')

    # 保存 MD5 校验失败的文件，便于脚本执行结束后进一步处理。
    failed_md5_file_list=[]

    # 第 2 步：定义 Rich 进度条的显示列，包括任务名、进度、百分比和耗时。
    progress_columns = [
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
    ]
    with Progress(*progress_columns) as progress:
        task_id = progress.add_task("Extracting...", total=len(sdf_files))
        for idx,sdf_file in enumerate(sdf_files):
            # 第 3 步：逐个解压文件；overwrite=True 表示允许覆盖已有解压结果。
            progress.update(task_id, description=f"Extracting {Path(sdf_file).name}")
            dest = ArchiveIO.extract(archive=sdf_file, dest_dir=input_dir,overwrite=True)

            # 第 4 步：仅当配套的“<压缩文件>.md5”存在时执行完整性校验。
            md5_file = Path(f"{str(sdf_file)}.md5")
            if md5_file.exists():
                result=verify_md5(sdf_file,md5_file)
                if not result:
                    failed_md5_file_list.append(sdf_file)
                print(f"{idx}:\t{result}{'✅' if result else '❌'}:\t {Path(sdf_file).name}")

            # 当前文件处理结束，推进总体进度。
            progress.update(task_id, advance=1)

    # 所有压缩文件处理完成后输出结束标记。
    print('finished')
