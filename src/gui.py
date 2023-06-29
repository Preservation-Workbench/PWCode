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
from rich.pretty import pprint as richprint
import pprint

console = Console()


def print_overwrite(msg):
    last_msg_length = len(print_overwrite.last_msg) if hasattr(print_overwrite, "last_msg") else 0
    print(" " * last_msg_length, end="\r")
    print(msg, end="\r")
    # sys.stdout.flush()
    print_overwrite.last_msg = msg


def print_msg(msg, style="bold red", exit=False, highlight=False):
    console.print(msg, style=style, highlight=highlight)
    if exit:
        sys.exit()


def show_output(cfg, obj, exit=True, error=False):
    gui = is_file = False

    if (os.name == "posix" and os.environ.get("DISPLAY")) or os.name == "nt":
        gui = True

    if type(obj).__name__ == "PosixPath" and obj.is_file():
        is_file = True

    if gui and not is_file:
        stdout_file = Path(str(cfg.tmp_dir), "std.out")
        with open(stdout_file, "w") as f:
            pprint.pprint(obj, stream=f)

        obj = stdout_file

    if gui:
        subprocess.call([cfg.editor, obj], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    else:
        if is_file:
            subprocess.call(["nano", obj])
        else:
            richprint(obj)

    if exit:
        if error:
            print_msg("Exit and open error message.", style="bold red", exit=exit)
        else:
            print_msg("Stopped for manual editing of '" + str(obj) + "'.", style="bold cyan", exit=exit)
