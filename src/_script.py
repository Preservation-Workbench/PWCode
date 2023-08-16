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
from dataclasses import replace
import importlib
import gui


def run(main_cfg):
    cfg = replace(main_cfg, **{"script_path": Path(main_cfg.script_path).resolve()})

    if cfg.script_path.parent.resolve() == cfg.scripts_dir.resolve():
        script = importlib.import_module(cfg.script_path.stem)
        script.run(cfg)
    else:
        gui.print_msg("Script file must be in script directory!", style=gui.style.warning)
