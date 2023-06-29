# Copyright (C) 2023 Morten Eek

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import blake3
import hashlib
import fileinput
import os
import sys
import subprocess
from pathlib import Path


def get_unique_dir(dir_path):
    path = Path(dir_path)

    i = 0
    while True:
        i += 1
        path = path if i == 1 else Path(str(path) + str(i))
        if not path.is_dir():
            return path


def get_unique_file(file_path):
    path = Path(file_path)

    i = 0
    while True:
        i += 1
        path = path if i == 1 else Path(str(path.with_suffix("")) + str(i) + str(path.suffix))
        if not path.is_file():
            return path


def replace_text_in_file(file_path, search_text, new_text):
    with fileinput.input(file_path, inplace=True) as f:
        for line in f:
            new_line = line.replace(search_text, new_text)
            print(new_line, end="")


def xdg_open_file(filename):
    if sys.platform == "win32":
        os.startfile(filename)
    else:
        opener = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, filename])


def get_checksum(filename, blocksize=65536):
    hash = blake3.blake3()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()


def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()
