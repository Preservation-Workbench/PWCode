# MIT License

# Original work Copyright (c) 2020 Open Knowledge Foundation
# Modified work Copyright 2023 Morten Eek

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import sys
import yaml
from pathlib import Path
from dataclasses import dataclass, fields

import gui
import configdb
import sqlite_utils
import jdk
import project


@dataclass()
class Main:
    cfg_file: Path
    command: str
    script_path: Path
    debug: bool
    stop: bool
    test: bool
    source: str
    target: str
    login_alias: dict
    jdbc_drivers: dict
    jar_files: list
    config_db: sqlite_utils.db.Database = None
    pwxtract_dir: Path = Path(__file__).resolve().parents[1]
    projects_dir: Path = Path(pwxtract_dir, "projects")
    editor: Path = Path(pwxtract_dir, "bin", "cuda", "cudatext")
    java_home: Path = Path(pwxtract_dir, "deps", "java." + jdk.OS)
    jars_dir: Path = Path(pwxtract_dir, "deps", "jars")

    def __post_init__(self):
        if jdk.OS == "windows":
            self.editor = Path(str(editor) + ".exe")


@dataclass()
class Copy(Main):
    source_type: str = None
    target_type: str = None
    project_dir: Path = None
    target_name: str = None
    source_db_path: Path = None
    target_db_path: Path = None
    content_dir: Path = None
    data_dir: Path = None
    tmp_dir: Path = None
    data_files_dir: Path = None


@dataclass()
class Archive(Main):
    tmp_dir: Path = None
    schema_path: Path = None
    # target_type: str = None
    # project_dir: Path = None
    # target_name: str = None
    # source_db_path: Path = None
    # target_db_path: Path = None
    # content_dir: Path = None
    # data_dir: Path = None
    # tmp_dir: Path = None
    # data_files_dir: Path = None


def load(cfg_file):
    configuration = dict()
    try:
        with open(cfg_file) as fil:
            configuration = yaml.load(fil, Loader=yaml.FullLoader)
    except PermissionError:
        pass
    except yaml.YAMLError as pe:
        gui.print_msg(f"""ERROR: Cannot parse the configuration file: {pe}""", exit=True)

    jdbc_drivers = dict()
    for jdbc_type, cfg in configuration.get("drivers", {}).items():
        if "jar" not in cfg:
            gui.print_msg(f"""ERROR in definition of driver type {jdbc_type}: jar file not specified.""", exit=True)
        elif "class" not in cfg:
            gui.print_msg(f"""ERROR in definition of driver type {jdbc_type}: driver class not specified.""", exit=True)
        elif "url" not in cfg:
            gui.print_msg(f"""ERROR in definition of driver type {jdbc_type}: url not specified.""", exit=True)

        jdbc_drivers[jdbc_type] = cfg

    jar_files = []
    for cfg in jdbc_drivers.values():
        if cfg["jar"] not in jar_files:
            jar_files.append(cfg["jar"])

    login_alias = configuration.get("aliases", {})

    return login_alias, jdbc_drivers, jar_files
