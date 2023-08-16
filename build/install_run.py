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
import runpy
import site
from pathlib import Path

base_dir = Path(os.environ["PYAPP"]).parent.absolute()
src_dir = Path(base_dir, "src")
deps_python_dir = Path(base_dir, "deps", "python")
scripts_dir = Path(base_dir, "scripts")

site_dirs = [src_dir, deps_python_dir, scripts_dir]
for s_dir in site_dirs:
    site.addsitedir(s_dir)

# INSTALL:
from install import run  # noqa: E402

run.install(base_dir, src_dir, deps_python_dir)

# RUN:
runpy.run_path(Path(src_dir, "main.py"), run_name="__main__")
