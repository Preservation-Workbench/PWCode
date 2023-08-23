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
import tarfile
import shutil
import os

from pathvalidate import replace_symbol
from command_runner import command_runner
from rich.prompt import Confirm
import configdb
import gui
import jdbc
import config
import project
import dp
import sqlwb
import db
from utils import _file
import _sqlite


def write_tables_file(table_file, cfg):
    if table_file.is_file():
        table_file.unlink()

    if cfg.data_files_dir:
        _sqlite.import_files(cfg)

    tables_count = jdbc.get_all_tables_count(cfg.source, cfg)
    if not tables_count:
        gui.print_msg("No tables in schema. Aborted.", exit=True)

    tables = []
    with open(table_file, "a") as fil:
        fil.write("# Included tables in source schema:\n")
        for table in tables_count:
            tables.append(table)
            fil.write(table + "\n")

    gui.print_msg("List of tables written to '" + str(table_file) + "'", style=gui.style.ok)

    return tables


def get_include_tables(cfg):
    changed = False
    first_run = True
    table_file = Path(cfg.tmp_dir, cfg.target_name + "-tables.txt")
    copied_tables = configdb.get_copied_tables(cfg)  # Already copied tables

    if copied_tables:
        first_run = False
        jdbc.get_all_tables_count(cfg.target, cfg, keys=False)

    if not table_file.is_file() or cfg.stop == "tables":
        tables = write_tables_file(table_file, cfg)
    else:
        with open(table_file) as file:
            tables = [s for s in file.read().splitlines() if not s.startswith("# ")]

        gui.print_msg("Export of table list to '" + str(table_file) + "' already done.", style=gui.style.info)

    configdb.update_include(cfg, tables)

    if cfg.stop == "tables":  # Edit included tables
        gui.show(cfg, table_file)  # Exits code for table editing

    if tables and not set(tables).issubset(copied_tables):
        configdb.update_table_deps(tables, cfg)
        jdbc.fix_column_size(tables, first_run, cfg)
        changed = True

    return changed


def get_copy_cfg(main_cfg):
    if main_cfg.source == main_cfg.target:
        gui.print_msg("Source and target cannot be the same. Aborted.", exit=True)

    target = main_cfg.target
    source, data_files_dir, source_db_path = project.get_data_dir_db(main_cfg)

    in_out = {"source": source, "target": target}
    for var_name, value in in_out.items():
        if value in main_cfg.login_alias:
            value = main_cfg.login_alias[value]

        if ":" in value:
            if (not value[0:5] == "jdbc:") or (value[-1] in ("/", "\\")):
                gui.print_msg("'" + value + "' is not a valid " + var_name + ". Aborted.", exit=True)

            in_out[var_name] = jdbc.get_conn(value, main_cfg)
        elif Path(value).is_dir():
            if Path(value) == main_cfg.projects_dir:
                gui.print_msg("'" + value + "' is not a valid " + var_name + ". Aborted.", exit=True)

            in_out[var_name] = value
        else:
            if var_name == "target":
                if len(replace_symbol(main_cfg.target)) != len(main_cfg.target.replace("_", "")):
                    gui.print_msg("'" + value + "' is not a valid " + var_name + ". Aborted.", exit=True)

                in_out[var_name] = value
            else:
                gui.print_msg("'" + value + "' is not a valid " + var_name + ". Aborted.", exit=True)

    source = in_out["source"]
    target = in_out["target"]
    source_type = None
    target_type = None

    if type(source).__name__ == "Dbo":
        source_type = source.type
        # source_schema = source.schema

        if type(target).__name__ == "Dbo":
            target_type = target.type
            # target_schema = target.schema

        elif Path(target).is_dir():
            target_type = "files"
            project_dir = Path(target)
        else:
            target_type = "project"
            project_dir = Path(main_cfg.projects_dir, target)
    else:
        source_type = "files"

        if type(target).__name__ == "Dbo":
            gui.print_msg("'" + target + "' is not av valid target for this source. Aborted.", exit=True)
        elif Path(target).is_dir():
            target_type = "files"
            project_dir = Path(target)
        else:
            target_type = "project"
            project_dir = Path(main_cfg.projects_dir, target)

    target_db_path, content_dir, target_name, data_dir, tmp_dir = project.get(source, target, source_type, target_type,
                                                                              project_dir)
    
    main_cfg_values = main_cfg.__dict__ | {'source': source, 'target': target, 'tmp_dir': tmp_dir}        
    cfg = config.Copy(
        **main_cfg_values,
        source_type=source_type,
        target_type=target_type,
        project_dir=project_dir,
        source_db_path=source_db_path,
        target_db_path=target_db_path,
        content_dir=content_dir,
        target_name=target_name,
        data_dir=data_dir,
        data_files_dir=data_files_dir,
    )

    return project.confirm(cfg)


def capture_files(cfg):
    copied = 0
    for row in cfg.config_db["files"].rows:
        tar_disk_path = Path(cfg.project_dir, row["tar_path"])

        if tar_disk_path.is_file() and tar_disk_path.stat().st_size == 0:
            tar_disk_path.unlink()

        if not tar_disk_path.is_file():
            cfg.config_db["files"].delete(row["source_path"])
            continue

        if row["source_path"] == cfg.source:
            copied = 2
            if (str(tar_disk_path.stat().st_mtime) == row["tar_mtime"]  # unmodified file (unmoved file)
                    or _file.get_checksum(tar_disk_path) == row["tar_checksum"]):  # Unmodified file (moved file)
                copied = 1

    if copied == 1:
        gui.print_msg("Directory '" + cfg.source + "' copied previously.", style=gui.style.info, highlight=True)
        return

    if copied == 2:
        ok = Confirm.ask("Copied ealier, but copy has been modified. Copy again?")
        if not ok:
            gui.print_msg("Aborted.", exit=True)

        tar_disk_path.unlink()
        cfg.config_db["files"].delete(cfg.source)

    gui.print_msg("Creating tar-file from source directory...", style=gui.style.info)

    cfg.target_tar_path = _file.get_unique_file(Path(cfg.content_dir, Path(cfg.source).name + ".tar"))
    if os.name == "posix" and shutil.which("tar") is not None:
        cmd = f"tar -cf {str(cfg.target_tar_path)} -C {str(Path(cfg.source).parent)} {Path(cfg.source).name}"
        exit_code, output = command_runner(cmd, encoding="utf-8")
        if exit_code != 0:
            gui.print_msg(output, exit=True)
    else:
        with tarfile.open(cfg.target_tar_path, mode="w") as archive:
            archive.add(cfg.source, arcname="")

    cfg.config_db["files"].insert(
        {
            "source_path": cfg.source,
            "tar_path": str(cfg.target_tar_path.relative_to(cfg.project_dir)),
            "tar_checksum": _file.get_checksum(cfg.target_tar_path),
            "tar_mtime": str(cfg.target_tar_path.stat().st_mtime),
            "tar_status": "created",
        },
        pk="source_path",
    )


def run(main_cfg):
    cfg = get_copy_cfg(main_cfg)

    if cfg.source_type in ("files", "project"):
        capture_files(cfg)
    else:
        # CREATE TARGET SCHEMA:
        changed = get_include_tables(cfg)  # Get source metadata
        schema_file = dp.create_schema(cfg, changed)  # Generate datapackage schema from source metadata
        diff_tables = configdb.tables_diff(cfg)  # Compare source and target DDL
        if diff_tables or cfg.stop == "ddl" or changed:  # Missing tables in target schema
            ddl_file = dp.create_ddl(schema_file, changed, cfg)  # Generate ddl
            sqlwb.run_ddl_file(cfg.target, cfg, diff_tables, ddl_file)  # Create target schema from generated ddl
        else:
            gui.print_msg("Target schema already created.", style=gui.style.info)

        # CÒPY DATA TO TARGET:
        diff_data = configdb.data_diff(cfg)  # Compare source and target data
        if diff_data or cfg.stop == "copy" or cfg.test:  # Missing data in target schema or test-run
            copy_file = sqlwb.get_copy_statements(schema_file, cfg, diff_data)  # Generate copy data statements
            sqlwb.run_copy_file(cfg, copy_file, diff_data)  # Copy data to target schema with copy data statements
        else:
            gui.print_msg("All data copied previously.", style=gui.style.info)

        # VERIFY COPIED DATA:
        if diff_data:
            diff_data = configdb.data_diff(cfg, count=True)  # Compare source and target data again after copying
            db.fix_fk(cfg)  # Add any missing foreign keys in target
            if diff_data:
                gui.print_msg("Something went wrong. Missing data in target!", exit=True)

            if cfg.test:
                gui.print_msg("Test run completed!", style=gui.style.ok)
            else:
                gui.print_msg("All data copied successfully!", style=gui.style.ok)

    return cfg
