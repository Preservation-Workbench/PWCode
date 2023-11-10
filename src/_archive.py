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
from collections import OrderedDict
import operator
import os
import sys
import json
import shutil
import csv
import fileinput

from utils import _file, _dict
from command_runner import command_runner
from rich.prompt import Confirm
import configdb
from frictionless import Package, validate
import petl as etl
import sqlwb
import dp
import gui
import jdbc
import config


def get_sources(main_cfg):
    source = main_cfg.source
    if not Path(source).is_dir():
        source = Path(main_cfg.projects_dir, source)

    sources = {}
    for pth in Path(source, "content").iterdir():
        db_path = Path(pth, pth.name + ".db")
        if pth.is_dir():
            if (pth.name != "documents"
                    and not Path(pth, "datapackage.json").is_file()  # Missing datapackage schema for db source
                    or any(Path(pth).iterdir()) is False  # Empty directory
                    or (pth.name != "documents" and not db_path.is_file())):  # Missing db for db source
                gui.print_msg("'" + str(source) + "' is not a valid source. Aborted.", exit=True)

            if pth.name == "documents":
                for files_pth in pth.iterdir():
                    if (files_pth.is_dir()
                            and any(files_pth.iterdir()) is True) or files_pth.suffix[1:].lower() == "tar":
                        sources[files_pth] = "dir" if files_pth.suffix == "" else files_pth.suffix[1:]
            else:
                sources[db_path] = db_path.suffix[1:]

    # Remove any tar source for extracted tar:
    for source in [x for x in sources if "dir" in sources[x]]:
        if source.name in [x.with_suffix("").name for x in sources if "tar" in sources[x]]:
            tar_path = Path(source.parent, source.name + ".tar")
            tar_path.unlink()
            sources.pop(tar_path)

    return OrderedDict(sorted(sources.items(), key=operator.itemgetter(1)))  # Ordered by type


def get_archive_cfg(source, main_cfg):
    target = main_cfg.target
    if not isinstance(target, str):
        target = main_cfg.source

    if not Path(target).is_dir():
        target = Path(main_cfg.projects_dir, target)

    if Path(target) == main_cfg.projects_dir:
        gui.print_msg("'" + str(target) + "' is not a valid target. Aborted.", exit=True)

    tmp_dir = Path(target, "tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    schema_path = Path(source.parent, "datapackage.json")
    config_db = ensure_config_db(Path(tmp_dir, source.parent.name + "-config.db"), schema_path)

    main_cfg_values = {}
    for key, value in main_cfg.__dict__.items():
        if key in ["source", "target", "config_db"]:
            continue

        main_cfg_values[key] = value

    main_cfg_values = main_cfg.__dict__ | {
        'source': source,
        'target': target,
        'tmp_dir': tmp_dir,
        'config_db': config_db
    }
    cfg = config.Archive(**main_cfg_values, schema_path=schema_path, content_dir=source.parent)

    return cfg


def export_file_column(dbo, table, file_column, cfg):
    documents_dir = Path(cfg.source.parent, "documents")
    documents_dir.mkdir(parents=True, exist_ok=True)
    data = dbo.query("SELECT rowid, " + file_column + " FROM " + table.name)

    for rowid, lob in data:
        file_path = Path(documents_dir, table.name + "_" + file_column + rowid + ".data")
        with open(file_path, "wb") as f:
            f.write(lob)


def export_text_columns(dbo, table, text_columns, tsv_path, cfg):
    # Use version in sqlwb.py for now because of performance issues (too slow and runs out of memory on big tables)
    def _fix_text(val):
        repls = {"\t": " ", "\n": "", "\r": ""}  # Replace tabs and linebreaks
        for key in repls:
            val = val.replace(key, repls[key])
        return val

    data = dbo.query("SELECT " + ",".join(text_columns.values()) + " FROM " + table.name)
    df = etl.convert([text_columns.keys(), *data], tuple(text_columns.keys()), _fix_text)
    etl.tocsv(df, tsv_path, delimiter="\t", skipinitialspace=True, quoting=csv.QUOTE_NONE, quotechar="", escapechar="")


def validate_tables(deps_list, table_deps, archived_tables, cfg):
    validated_tables = configdb.get_validated_tables(cfg)
    deps_list = list(set(deps_list))
    for table in deps_list:
        if [table] == table_deps[table]:
            for tbl_list in {k: v for k, v in table_deps.items() if k not in archived_tables}.values():
                if table in tbl_list:
                    deps_list.remove(table)  # table will be validated later, avoid doing it multiple times
                    break

    if len(deps_list) == 0 or all(item in validated_tables for item in deps_list):
        return 0, validated_tables, deps_list

    gui.print_msg("Validating dependent tables against datapackage schema...", style=gui.style.info)

    schema_path = Path(cfg.source.parent, "partial_datapackage.json")
    report = validate(dp.create_schema(cfg, True, tables=deps_list, schema_path=schema_path))
    if report.valid is False:
        gui.show(cfg, report, exit=True, error=True)

    for table in deps_list:
        cfg.config_db["tables"].update(table, {"validated": 1})

    idx = 0
    deps_list = []

    if schema_path.is_file():
        schema_path.unlink()

    return idx, validated_tables.extend(deps_list), deps_list


def ensure_config_db(db_path, schema_path):
    config_db = configdb.create_db(db_path)

    if config_db["tables"].count == 0:
        with open(schema_path) as f:
            package = Package(json.load(f))
            deps_order = 0
            for table in package.resources:
                deps_order += 1
                config_db["tables"].insert(
                    {
                        "source_name": table.custom["db_table_name"],
                        "norm_name": table.name,
                        "source_row_count": table.custom["count_of_rows"],
                        "source_pk": ",".join(table.schema.primary_key),
                        "deps": table.custom["db_table_deps"],
                        "deps_order": deps_order,
                        "include": 1,
                        "created": 1,
                    },
                    pk="source_name",
                )

                col_pos = 0
                for field in table.schema.fields:
                    col_pos += 1
                    config_db["columns"].insert(
                        {
                            "tbl_col_pos": table.custom["db_table_name"] + "*" + str(col_pos),
                            "source_table": table.custom["db_table_name"],
                            "source_column": field.custom["db_column_name"],
                            "norm_column": field.name,
                            "jdbc_data_type": int(field.custom["jdbc_type"]),
                            "source_column_position": col_pos,
                        },
                        pk="tbl_col_pos",
                    )

        for row in config_db["tables"].rows:  # Update normalized key columns to source
            norm_pk_list = row["source_pk"].split(",")
            source_pk_list = []
            for norm_key in norm_pk_list:
                source_key = config_db.execute(f"""
                    SELECT source_column
                    FROM columns
                    WHERE source_table = '{row["source_name"]}'
                    AND   norm_column = '{norm_key}'
                    """).fetchone()[0]
                source_pk_list.append(source_key)

            config_db["tables"].update(row["source_name"], {"source_pk": ",".join(source_pk_list)})

    return config_db


def archive_db(source, main_cfg):
    cfg = get_archive_cfg(source, main_cfg)
    has_empty_rows = False

    export_blobs = True
    sub_system = configdb.get_sub_system(cfg.content_dir.name, cfg.config_db)
    if "--no-blobs" in sub_system.args:
        export_blobs = False

    gui.print_msg("Exporting '" + cfg.source.name + "' to tsv-files:", style=gui.style.info)

    changed = False
    data_dir = Path(cfg.source.parent, "data")
    data_dir.mkdir(parents=True, exist_ok=True)
    dbo = jdbc.get_conn("jdbc:sqlite:" + str(cfg.source), cfg)
    validated_tables = configdb.get_validated_tables(cfg)
    table_deps = configdb.get_tables_deps(cfg)
    norm_tables = configdb.get_norm_tables(cfg.config_db)
    archived_tables = []
    deps_list = []

    with open(cfg.schema_path) as f:
        package = Package(json.load(f))
        idx = 0
        for table in package.resources:
            tsv_path = Path(data_dir, table.name + ".tsv")

            if tsv_path.is_file():
                if tsv_path.stat().st_size == 0:
                    tsv_path.unlink()
                else:
                    gui.print_msg("'" + table.path + "' already exported.", style=gui.style.info, highlight=True)
                    archived_tables.append(table.custom["db_table_name"])
                    continue
            else:
                cfg.config_db["tables"].update(table.custom["db_table_name"], {"validated": 0})

            gui.print_msg(
                "Writing '" + table.path + "' (" + table.custom["count_of_rows"] + " rows)...",
                style=gui.style.info,
                highlight=True,
            )

            file_columns = []
            text_columns = {}
            changed = True
            for field in table.schema.fields:
                jdbc_data_type = field.custom["jdbc_type"]

                max_length = 0
                if "maxLength" in field.constraints.keys():
                    max_length = field.constraints["maxLength"]

                # Check for blobs and big clobs that should be exported as separate files
                if (export_blobs
                        and jdbc_data_type in [-4, -3, -2, 2004]) or (jdbc_data_type in [-16, -1, 2005, 2009, 2011]
                                                                      and max_length > 4000):
                    file_columns.append(field.name)
                    text_columns[field.name] = ("(SELECT " + table.name + "_" + field.name + " || rowid || .data AS " +
                                                field.name + ")")
                else:
                    text_columns[field.name] = field.name

            fix_table(dbo, table, text_columns, cfg)
            result = sqlwb.export_text_columns(dbo, table, text_columns, tsv_path, cfg)

            if str(result) == "Error":
                if tsv_path.is_file():
                    tsv_path.unlink()

                gui.print_msg(str(result), exit=True)

            tsv_row_count = tsv_fix(tsv_path)
            db_row_count = int(table.custom["count_of_rows"])
            if db_row_count > tsv_row_count:
                empty_rows = str(db_row_count - tsv_row_count)
                has_empty_rows = True
                source_table = _dict.get_key_from_value(norm_tables, table)
                cfg.config_db["tables"].update(source_table, {"empty_rows": empty_rows})

            for file_column in file_columns:
                export_file_column(dbo, table, file_column, cfg)

            idx += 1
            archived_tables.append(table.custom["db_table_name"])
            deps_list.extend(table.custom["db_table_deps"].split(","))
            if all(item in archived_tables for item in deps_list) and idx > 10:
                idx, validated_tables, deps_list = validate_tables(deps_list, table_deps, archived_tables, cfg)

        if len(deps_list) > 0:
            validate_tables(deps_list, table_deps, archived_tables, cfg)

    if has_empty_rows:
        dp.create_schema(cfg, True)

    if changed:
        gui.print_msg("Datapackage validated!", style=gui.style.ok)
    else:
        gui.print_msg("Datapackage already validated.", style=gui.style.info)


def fix_table(dbo, table, text_columns, cfg):
    print("Removing any null bytes before exporting data...")

    for text_column in text_columns.keys():
        sql = ("UPDATE " + table.name + " SET " + text_column + " = substr(" + text_column + ",1,instr (" +
               text_column + ",CHAR(0)) - 1) || substr(CAST(" + text_column + " AS BLOB),instr (" + text_column +
               ",CHAR(0)) + 1)")

        dbo.execute(sql)
        dbo.commit()


def tsv_fix(tsv_path):
    print("Removing any empty lines...")

    tsv_row_count = 0
    with fileinput.input(tsv_path, inplace=True) as f:
        for line in f:
            if len(line.replace("\t", "")) > 1:
                tsv_row_count += 1
                sys.stdout.write(line)

    return tsv_row_count


def archive_dir(source, cfg):
    pass
    # print(source)


def archive_tar(source, cfg):
    gui.print_msg("Checking '" + source.name + "' for modified content...", style=gui.style.info)

    state = 0  # Untracked, modified unknown
    config_db = configdb.create_db(Path(cfg.tmp_dir, "documents-config.db"))
    for row in config_db["files"].rows:
        tar_disk_path = Path(cfg.source, row["tar_path"])
        if tar_disk_path == source:  # tar-file in configdb
            new_tar_mtime = str(tar_disk_path.stat().st_mtime)
            if new_tar_mtime == row["tar_mtime"]:  # Not moved since last check->state 1
                state = 1
            elif _file.get_checksum(tar_disk_path) == row["tar_checksum"]:  # Moved but content unchanged->state 2
                config_db["files"].update(row["source_path"], {"tar_mtime": new_tar_mtime})  # Avoid checksum next run
                state = 2
            else:  # Modified -> state 3
                state = 3

            break

    if state == 0:
        gui.print_msg("Untracked file! Generating checksum for future reference...", style=gui.style.warning)
        cfg.config_db["files"].insert(
            {
                "source_path": source,
                "tar_path": str(source.relative_to(cfg.source)),
                "tar_checksum": _file.get_checksum(source),
                "tar_mtime": str(source.stat().st_mtime),
                "tar_status": "created",
            },
            pk="source_path",
        )
    elif state in [1, 2]:
        gui.print_msg("File verified!", style=gui.style.ok)
    elif state == 3:
        ok = Confirm.ask("File has been modified on disk! Proceed?")
        if not ok:
            gui.print_msg("Aborted.", exit=True)

    if os.name == "posix" and shutil.which("tar") is not None:  # Extract with gnu tar
        cmd = f"tar -xf {str(source)} -C {str(Path(source).parent)}"
        exit_code, output = command_runner(cmd, encoding="utf-8")
        if exit_code != 0:
            gui.print_msg(output, exit=True)
            # TODO: Slette feilaktig eksportert mappe her eller lenger nede så samme for pytar og gnutar!
        # else:
        # TODO: Slette tar-fil her eller først senere? -> Riktig ift sjekker tidligere om eksportert tar?

    # args.target_tar_path = _file.get_unique_file(Path(args.content_dir, Path(args.source).name + ".tar"))
    # if os.name == "posix" and (lambda x: shutil.which("tar") is not None):
    # cmd = f"tar -cf {str(args.target_tar_path)} -C {str(Path(args.source).parent)} {Path(args.source).name}"
    # exit_code, output = command_runner(cmd, encoding="utf-8")
    # if exit_code != 0:
    # gui.print_msg(output, args, exit=True)
    # else:
    # with tarfile.open(args.target_tar_path, mode="w") as archive:
    # archive.add(args.source, arcname="")


def run(main_cfg):
    sources = get_sources(main_cfg)

    funcs = {
        "db": (lambda source, main_cfg: archive_db(source, main_cfg)),
        "tar": (lambda source, main_cfg: archive_tar(source, main_cfg)),
        "dir": (lambda source, main_cfg: archive_dir(source, main_cfg)),
    }

    for source, source_type in sources.items():  # Ordered by source_type
        funcs[source_type](source, main_cfg)
