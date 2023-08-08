# Copyright(C) 2023 Morten Eek

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import subprocess
from pathlib import Path
from dataclasses import dataclass

from install import funcs


@dataclass()
class Config:  # TODO: Get some values from config file!
    base_dir: Path
    src_dir: Path
    deps_python_dir: Path
    tmp_dir: Path = None
    deps_java_dir: Path = None
    deps_jar_dir: Path = None
    deps_editor_dir: Path = None
    shfmt_bin: Path = None
    editor_url: str = "https://github.com/zyedidia/micro/releases/download/v2.0.11/micro-2.0.11-linux64.tar.gz"
    shfmt_url: str = "https://github.com/mvdan/sh/releases/download/v3.7.0/shfmt_v3.7.0_linux_amd64"
    java_version: str = "11"

    def __post_init__(self):
        self.deps_java_dir = Path(self.base_dir, "deps", "java.linux")
        self.deps_jar_dir = Path(self.base_dir, "deps", "jars")
        self.deps_editor_dir = Path(self.base_dir, "deps", "editor")
        self.shfmt_bin = Path(self.deps_editor_dir, "deps", "shfmt")
        self.tmp_dir = Path(self.base_dir, "projects", "tmp")
        if os.name != "posix":
            self.editor_url: str = "https://github.com/zyedidia/micro/releases/download/v2.0.11/micro-2.0.11-win64.zip"
            self.deps_java_dir: Path = Path(self.base_dir, "deps", "java.windows")
            self.shfmt_url: str = "https://github.com/mvdan/sh/releases/download/v3.7.0/shfmt_v3.7.0_windows_amd64.exe"
            self.shfmt_bin = Path(self.deps_editor_dir, "deps", "shfmt.exe")


def install(base_dir, src_dir, deps_python_dir):
    cfg = Config(base_dir=base_dir, src_dir=src_dir, deps_python_dir=deps_python_dir)

    cfg.tmp_dir.mkdir(parents=True, exist_ok=True)
    Path(cfg.tmp_dir, ".gitkeep").touch(exist_ok=True)

    cfg.deps_python_dir.mkdir(parents=True, exist_ok=True)
    Path(cfg.deps_python_dir, ".gitkeep").touch(exist_ok=True)

    # PYTHON:
    if len(os.listdir(cfg.deps_python_dir)) == 1:
        # rgb(138, 173, 244)
        print("\033[38;2;{};{};{}m{} \033[39m".format(138, 173, 244, "Installing python dependencies..."))
        req_file = Path(cfg.base_dir, "requirements.txt")
        cmd = [sys.executable, "-m", "pip", "install", "--target", cfg.deps_python_dir, "-r", req_file]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, universal_newlines=True)
        result = proc.communicate()[1]
        if "ERROR:" in result:
            print("\033[38;2;{};{};{}m{} \033[39m".format(237, 135, 150, result))  # rgb(237, 135, 150)
            sys.exit()

    # THE REST:
    for func in funcs.__dict__.values():
        if callable(func) and func.__module__ == funcs.__name__:
            func(cfg)
