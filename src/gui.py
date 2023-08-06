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

import os
import sys
import subprocess
from pathlib import Path

from rich.console import Console
from dataclasses import dataclass
import pprint


@dataclass()
class Style:
    warning: str = "#ED8796"
    warning_bold: str = "bold #ED8796"
    ok: str = "#A6DA95"
    ok_bold: str = "bold #A6DA95"
    info: str = "#8AADF4"
    info_bold: str = "bold #8AADF4"


style = Style()
console = Console()


def print_overwrite(msg):
    last_msg_length = len(print_overwrite.last_msg) if hasattr(print_overwrite, "last_msg") else 0
    print(" " * last_msg_length, end="\r")
    print(msg, end="\r")
    # sys.stdout.flush()
    print_overwrite.last_msg = msg


def print_msg(msg, style=style.warning, exit=False, highlight=False):
    console.print(msg, style=style, highlight=highlight)
    if exit:
        sys.exit()


def show(cfg, obj, exit=True, error=False):

    if not (type(obj).__name__ == "PosixPath" and obj.is_file()):
        stdout_file = Path(str(cfg.tmp_dir), "std.out")
        with open(stdout_file, "w") as f:
            pprint.pprint(obj, stream=f)

        obj = stdout_file

    env = os.environ.copy()
    env["PATH"] = str(Path(cfg.pwxtract_dir, "deps", "python", "bin")) + os.pathsep + env["PATH"]
    env["PYTHONPATH"] = str(Path(cfg.pwxtract_dir, "deps", "python"))
    subprocess.call([cfg.editor, "--config-dir", Path(cfg.editor.parent, "config"), obj], env=env)

    if exit:
        if error:
            print_msg("Exit and open error message.", style=style.warning, exit=exit)
        else:
            print_msg("Stopped for manual editing of '" + str(obj) + "'.", style=style.info, exit=exit)
