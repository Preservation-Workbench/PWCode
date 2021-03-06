import os
import sys
from pathlib import Path
from common.file import get_checksum
from common.process_metadata_pre import normalize_metadata
from common.process_metadata_check import load_data
from common.xml_settings import XMLSettings
import tarfile
from defs import (  # .defs.py
    normalize_data
)


def main():
    bin_dir = os.environ["pwcode_bin_dir"]  # Get PWCode executable path
    java_path = os.environ['pwcode_java_path']  # Get Java home path
    class_path = os.environ['CLASSPATH']  # Get jar path
    config_dir = os.environ["pwcode_config_dir"]  # Get PWCode config path
    tmp_dir = os.path.join(config_dir, 'tmp')
    os.chdir(tmp_dir)  # Avoid littering from subprocesses
    project_dir = str(Path(__file__).parents[2])
    config_path = os.path.join(project_dir, 'pwcode.xml')
    config = XMLSettings(config_path)
    project_name = config.get('name')
    package = config.get('options/create_package')
    memory = '-Xmx' + config.get('options/memory').split(' ')[0] + 'g'
    archive = os.path.join(project_dir, project_name + '.tar')

    if package == 'Yes':
        if not os.path.isfile(archive):
            return "'" + archive + "' does not exist. Exiting."

        checksum = config.get('checksum')
        checksum_verified = config.get('checksum_verified')

        if not checksum:
            return "No checksum in config file. Exiting."

        if not checksum_verified == 'Yes':
            if checksum == get_checksum(archive):
                print("Data verified by checksum.")
                config.put('checksum_verified', 'Yes')
                config.save()
            else:
                return "Checksum mismatch. Check data integrity. Exiting."

    sub_systems_dir = os.path.join(project_dir, 'content', 'sub_systems')
    extracted = False
    if os.path.isdir(sub_systems_dir):
        if len(os.listdir(sub_systems_dir)) != 0:
            extracted = True

    if not extracted:
        with tarfile.open(archive) as tar:
            tar.extractall(path=project_dir)

    # TODO: Hent java path
    result = normalize_data(project_dir, bin_dir, class_path, java_path, memory, tmp_dir)
    if result == 'Error':
        return result

    result = normalize_metadata(project_dir, config_dir)
    load_data(project_dir, config_dir)

    return 'Juhuu :)'

# TODO: Legg inn sjekk på om finnes upakket mappestruktur hvis ikke tar. Ha i config om pakket eller ikke? Ja -> sjekksum henger jo sammen med den


if __name__ == '__main__':
    result = main()
    if result == 'Error':
        print(result, file=sys.stderr)
    else:
        print(result)
