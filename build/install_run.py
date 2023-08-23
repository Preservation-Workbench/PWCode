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
import runpy
import site
from pathlib import Path
import subprocess

base_dir = Path(os.environ["PYAPP"]).parent.absolute()
src_dir = Path(base_dir, "src")
deps_python_dir = Path(base_dir, "deps", "python")
scripts_dir = Path(base_dir, "scripts")

deps_python_dir.mkdir(parents=True, exist_ok=True)
Path(deps_python_dir, ".gitkeep").touch(exist_ok=True)

if len(os.listdir(deps_python_dir)) == 1:
    # rgb(138, 173, 244)
    print("\033[38;2;{};{};{}m{} \033[39m".format(138, 173, 244, "Installing python dependencies..."))
    req_file = Path(base_dir, "requirements.txt")
    cmd = [sys.executable, "-m", "pip", "install", "--target", deps_python_dir, "-r", req_file]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, universal_newlines=True)
    result = proc.communicate()[1]
    if "ERROR:" in result:
        print("\033[38;2;{};{};{}m{} \033[39m".format(237, 135, 150, result))  # rgb(237, 135, 150)
        sys.exit()

site_dirs = [src_dir, deps_python_dir, scripts_dir]
for s_dir in site_dirs:
    site.addsitedir(s_dir)

runpy.run_path(Path(src_dir, "main.py"), run_name="__main__")
