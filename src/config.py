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

import yaml
import os
from pathlib import Path
from dataclasses import dataclass

import gui
import sqlite_utils
import jdk


@dataclass()
class Main:  # TODO: Get some values from config file!
    cfg_file: Path
    command: str
    script_path: Path
    file_path: Path
    debug: bool
    stop: bool
    no_blobs: bool
    schema: str
    test: bool
    source: str
    target: str
    login_alias: dict
    jdbc_drivers: dict
    jar_files: list
    config_db: sqlite_utils.db.Database = None
    pwcode_dir: Path = Path(os.getenv("pwcode_dir"))
    tmp_dir: Path = Path(pwcode_dir, "projects", "tmp")
    projects_dir: Path = Path(pwcode_dir, "projects")
    java_home: Path = Path(pwcode_dir, "deps", "java." + jdk.OS)
    scripts_dir: Path = Path(os.getenv("pwcode_scripts_dir"))
    src_dir: Path = Path(os.getenv("pwcode_src_dir"))
    python_dir: Path = Path(os.getenv("pwcode_python_dir"))
    editor_url: str = "https://github.com/zyedidia/micro/releases/download/v2.0.12/micro-2.0.12-linux64-static.tar.gz"
    shfmt_url: str = "https://github.com/mvdan/sh/releases/download/v3.7.0/shfmt_v3.7.0_linux_amd64"
    rg_url: str = "https://github.com/BurntSushi/ripgrep/releases/download/13.0.0/"\
        "ripgrep-13.0.0-x86_64-unknown-linux-musl.tar.gz"
    fzf_url: str = "https://github.com/junegunn/fzf/releases/download/0.42.0/fzf-0.42.0-linux_amd64.tar.gz"
    ctags_url: str = "https://github.com/universal-ctags/ctags-nightly-build/releases/download/"\
        "2023.08.13%2Bce46d93811faefaa75b87e334d768fbf9a831861/uctags-2023.08.13-linux-x86_64.tar.xz"
    java_version: str = "11"
    java_dir: Path = Path(pwcode_dir, "deps", "java.linux")
    java_bin: Path = Path(pwcode_dir, "deps", "java.linux", "bin", "java")
    jars_dir: Path = Path(pwcode_dir, "deps", "jars")
    sqlwb_bin: Path = Path(jars_dir, "sqlworkbench.jar")
    editor_dir: Path = Path(pwcode_dir, "deps", "editor")
    editor_bin: Path = Path(editor_dir, "micro")
    shfmt_bin: Path = Path(editor_dir, "deps", "shfmt")
    fzf_bin: Path = Path(editor_dir, "deps", "fzf")
    rg_bin: Path = Path(editor_dir, "deps", "rg")
    ctags_bin: Path = Path(editor_dir, "deps", "ctags")

    def __post_init__(self):
        if jdk.OS == "windows":
            self.editor_url = "https://github.com/zyedidia/micro/releases/download/v2.0.12/micro-2.0.12-win64.zip"
            self.java_dir = Path(self.pwcode_dir, "deps", "java.windows")
            self.java_bin = Path(self.pwcode_dir, "deps", "java.windows", "bin", "java.exe")
            self.shfmt_url = "https://github.com/mvdan/sh/releases/download/v3.7.0/shfmt_v3.7.0_windows_amd64.exe"
            self.rg_url = "https://github.com/BurntSushi/ripgrep/releases/download/"\
                "13.0.0/ripgrep-13.0.0-x86_64-pc-windows-msvc.zip"
            self.fzf_url = "https://github.com/junegunn/fzf/releases/download/0.42.0/fzf-0.42.0-windows_amd64.zip"
            self.ctags_url = "https://github.com/universal-ctags/ctags-win32/releases/download/"\
                "p6.0.20230813.0/ctags-p6.0.20230813.0-x64.zip"
            self.editor_bin = Path(self.editor_dir, "micro.exe")
            self.shfmt_bin = Path(self.editor_dir, "deps", "shfmt.exe")
            self.fzf_bin = Path(self.editor_dir, "deps", "fzf.exe")
            self.rg_bin: Path = Path(self.editor_dir, "deps", "rg.exe")
            self.ctags_bin: Path = Path(self.editor_dir, "deps", "ctags.exe")


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
    data_files_dir: Path = None


@dataclass()
class Archive(Main):
    schema_path: Path = None
    # target_type: str = None
    # project_dir: Path = None
    # target_name: str = None
    # source_db_path: Path = None
    # target_db_path: Path = None
    # content_dir: Path = None
    # data_dir: Path = None
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
