# Copyright(C) 2022 Morten Eek

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

from pathlib import Path
import subprocess


def run(cfg):
    cmd = [
        cfg.java_bin,
        "-Djava.awt.headless=true",
        "-Dvisualvm.display.name=SQLWorkbench/J",
        "-cp",
        f'{str(cfg.sqlwb_bin)}:{str(Path(cfg.jars_dir,"ext")) + "/*"}',
        "workbench.console.SQLConsole",
        f"-configDir={str(cfg.jars_dir)}",
    ]

    subprocess.call(
        cmd,
        universal_newlines=True,
    )
