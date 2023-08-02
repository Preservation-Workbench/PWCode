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

import sys
from pathlib import Path
from dataclasses import replace

import gui
import config
from specific_import import import_file


def run(main_cfg):
    cfg = config.Edit(**main_cfg.__dict__, tmp_dir=Path(main_cfg.projects_dir, "tmp"))
    gui.show(cfg, Path(main_cfg.file_path).resolve())
