# Copyright (C) 2022 Morten Eek

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

# Python Library Imports:
import os
import re
from pathvalidate import sanitize_filename
from pathlib import Path

from rich.prompt import Confirm
from sqlite_utils import Database
from dataclasses import replace
import configdb
import jdbc
import gui
import _sqlite


def confirm(cfg):
    for key, value in cfg.__dict__.items():
        if key not in ("source", "target", "data_files_dir", "target_db_path", "source_schema", "stop"):
            continue

        if key == "source" and type(cfg.source).__name__ == "Dbo":
            value = cfg.source.url[cfg.source.url.rindex(":") + 1 :]

        if key == "target" and type(cfg.target).__name__ == "Dbo":
            value = cfg.target.url[cfg.target.url.rindex(":") + 1 :]

        if key in ["data_files_dir", "stop"] and value is None:
            continue

        if cfg.source_type in ["files", "project"] and key == "target_db_path":
            continue

        if cfg.source_type in ["files", "project", "sqlite", "interbase"] and key == "source_schema":
            continue

        if (
            cfg.source_type not in ["files", "project"]
            and cfg.target_type not in ["sqlite", "interbase"]
            and key == "target_db_path"
        ):
            continue

        gui.print_msg(str(key) + ": " + str(value), style=gui.style.info, highlight=True)

    print()
    msg = "Check arguments! Continue?"
    if (Path(cfg.project_dir).is_dir()) and (len(os.listdir(cfg.project_dir)) > 0):
        msg = "Non-empty project directory. Continue anyway?"

    ok = Confirm.ask(msg)
    if not ok:
        gui.print_msg("Aborted", exit=True)

    sub_dirs = [cfg.tmp_dir, cfg.content_dir]
    if cfg.source_type not in ("files", "project"):
        sub_dirs.append(cfg.data_dir)

    for sub_dir in sub_dirs:
        sub_dir.mkdir(parents=True, exist_ok=True)

    config_db = configdb.create_db(Path(cfg.tmp_dir, cfg.target_name + "-config.db"))
    target = cfg.target
    if cfg.target_db_path:
        target = get_target_db(cfg.target_db_path, cfg)

    return replace(cfg, **{"config_db": config_db, "target": target})


def get_target_db(target_db_path, cfg):
    if not Path(target_db_path).is_file:
        Database(target_db_path).enable_wal()

    return jdbc.get_conn("jdbc:sqlite:" + str(target_db_path), cfg)


def get_data_dir_db(main_cfg):
    data_files_dir = None
    source_db_path = None
    source = main_cfg.source
    if Path(source).is_dir():
        ext_funcs = _sqlite.ext_cmds()
        files = Path(source).rglob("*")
        f_count = len([f for f in files if f.is_file()])
        d_count = 0
        for fil in Path(source).rglob("*"):
            if fil.suffix[1:].lower() in ext_funcs.keys():
                d_count += 1

            if d_count > int(f_count / 2):
                data_files_dir = Path(source)
                break

    if data_files_dir:
        project_dir = Path(main_cfg.projects_dir, main_cfg.target)
        tmp_dir = Path(project_dir, "tmp")
        source_db_path = Path(tmp_dir, project_dir.name + ".db")
        source = "jdbc:sqlite:" + str(source_db_path)
        if not Path(source_db_path).is_file():
            tmp_dir.mkdir(parents=True, exist_ok=True)
            Database(str(source_db_path), use_counts_table=True)

        source = "jdbc:sqlite:" + str(source_db_path)

    return source, data_files_dir, source_db_path


def get(source, target, source_type, target_type, project_dir):
    target_db_path = None
    content_dir = None
    target_name = None

    if source_type in ("files", "project"):
        target_name = "documents"
    else:
        if source_type in ("sqlite", "interbase"):
            target_name = sanitize_filename(str(Path(Path(source.url).parent.name, Path(source.url).stem)).lower())
        else:
            url = source.url
            for i in [" ", "."]:
                url = url.replace(i, "")

            if source_type == "oracle":
                target_name = (
                    sanitize_filename(re.sub(":.*?:", "", url.split("@")[-1].lower())) + "-" + source.schema.lower()
                )
            else:
                target_name = sanitize_filename(
                    re.split("\\bjdbc:" + source_type + "://\\b", url)[-1].partition(":")[0].lower()
                    + "-"
                    + source.schema.lower()
                )

        target_name = re.sub(r"[^A-Za-z0-9]", "", target_name)

    if target_type in ("files", "project"):
        content_dir = Path(project_dir, "content", target_name)
        if source_type not in ("files", "project"):
            target_db_path = Path(content_dir, target_name + ".db")
    else:
        project_dir = Path(project_dir, target_name)
        content_dir = Path(project_dir, "content", target_name)

    data_dir = Path(content_dir, "data")
    tmp_dir = Path(project_dir, "tmp")

    return target_db_path, content_dir, target_name, data_dir, tmp_dir
