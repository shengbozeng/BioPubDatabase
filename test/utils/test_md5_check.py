from pathlib import Path
from nih.pubchem.utils import verify_md5,get_files_by_extension
if __name__=="__main__":
    input_dir = '/data/pubchem_origin_data/compound_current-full_sdf'
    sdf_files=get_files_by_extension(input_dir,extension='.gz')

    for i,sdf_file in enumerate(sdf_files):      
        gz_file = Path(sdf_file)
        md5_file =Path(f"{sdf_file}.md5")
        result=verify_md5(gz_file,md5_file)
        print(f"{i}:\t{result}{'✅' if result else '❌'}:\t {gz_file.name}")

    print('finished.')