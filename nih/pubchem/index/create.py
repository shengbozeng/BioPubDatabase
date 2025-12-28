from pathlib import Path
from ailingues_core.utils.archive_io import ArchiveIO, ArchiveType
from nih.pubchem.utils import verify_md5, get_files_by_extension
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn


if __name__=="__main__":
    input_dir = Path('/data/pubchem_more_origin_data/compound_currentfull_sdf/')
    sdf_files=get_files_by_extension(str(input_dir),extension='.gz')

    progress_columns = [
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
    ]
    with Progress(*progress_columns) as progress:
        task_id = progress.add_task("Extracting...", total=len(sdf_files))
        for sdf_file in sdf_files:
            progress.update(task_id, description=f"Extracting {Path(sdf_file).name}")
            dest = ArchiveIO.extract(archive=sdf_file, dest_dir=input_dir)
            progress.update(task_id, advance=1)

    print('finished')
    
