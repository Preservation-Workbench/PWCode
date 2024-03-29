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
import tomllib
import site
from pathlib import Path
import subprocess

pwcode_dir = Path(os.environ["PYAPP"]).parent.absolute()
src_dir = Path(pwcode_dir, "src")
deps_python_dir = Path(pwcode_dir, "deps", "python")
scripts_dir = Path(pwcode_dir, "scripts")

os.environ['pwcode_dir'] = str(pwcode_dir)
os.environ['pwcode_src_dir'] = str(src_dir)
os.environ['pwcode_python_dir'] = str(deps_python_dir)
os.environ['pwcode_scripts_dir'] = str(scripts_dir)

deps_python_dir.mkdir(parents=True, exist_ok=True)
Path(deps_python_dir, ".gitkeep").touch(exist_ok=True)

if len(os.listdir(deps_python_dir)) == 1:
    data = tomllib.load(open(Path(pwcode_dir, "pyproject.toml"), "rb"))
    deps = data["project"].get("dependencies")
    if deps:
        # rgb(138, 173, 244)
        print("\033[38;2;{};{};{}m{} \033[39m".format(138, 173, 244, "Installing python dependencies..."))
        cmd = [sys.executable, "-m", "pip", "install", *deps, "--target", deps_python_dir]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, universal_newlines=True)
        result = proc.communicate()[1]
        if "ERROR:" in result:
            print("\033[38;2;{};{};{}m{} \033[39m".format(237, 135, 150, result))  # rgb(237, 135, 150)
            sys.exit()

site_dirs = [src_dir, deps_python_dir, scripts_dir]
for s_dir in site_dirs:
    site.addsitedir(s_dir)

runpy.run_path(src_dir, run_name="__main__")
