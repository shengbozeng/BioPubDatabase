from pathlib import Path
from ailingues_core.utils.archive_io import ArchiveIO, ArchiveType

from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from utils.files import get_files_by_extension
from utils.md5_check import verify_md5


if __name__=="__main__":
    input_dir = Path('./nih/pubchem/index/test_index')
    sdf_files=get_files_by_extension(str(input_dir),extension='.gz')
    failed_md5_file_list=[]
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
            progress.update(task_id, description=f"Extracting {Path(sdf_file).name}")
            dest = ArchiveIO.extract(archive=sdf_file, dest_dir=input_dir,overwrite=True)
            md5_file = Path(f"{str(sdf_file)}.md5")
            if md5_file.exists():
                result=verify_md5(sdf_file,md5_file)
                if not result:
                    failed_md5_file_list.append(sdf_file)
                print(f"{idx}:\t{result}{'✅' if result else '❌'}:\t {Path(sdf_file).name}")
            
            progress.update(task_id, advance=1)

    print('finished')
    
