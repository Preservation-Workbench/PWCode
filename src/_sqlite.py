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

import os
import sqlite3
import warnings
import shutil
from pathlib import Path

import petl as etl
import openpyxl as op
import db
import configdb
from command_runner import command_runner
import gui

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def ext_cmds():
    """
    Get supported tabular files extensions and function to use for each.
    """
    ext_funcs = {
        "json": (lambda fil, index, conn, args: import_json(fil, index, conn, args)),
        "xlsx": (lambda fil, index, conn, args: import_excel(fil, index, conn, args)),
    }

    if os.name == "posix" and (lambda x: shutil.which("soffice") is not None):
        ext_funcs["ods"] = lambda fil, index, conn, args: import_ods(fil, index, conn, args)

    return ext_funcs


def import_excel(fil, index, conn, args):
    """
    Import xlsx files in sqlite database
    """
    wb = op.load_workbook(fil, read_only=True, data_only=True)
    for ws_name in wb.sheetnames:
        for row in wb[ws_name].values:
            if all(value is None for value in row):
                continue  # Ignore empty sheets

        table_name = db.normalize_name(ws_name, index, length=True)
        gui.print_msg("Creating and populating table '" + table_name + "'", style=gui.style.info, highlight=True)
        table = etl.io.xlsx.fromxlsx(fil, sheet=ws_name, read_only=True)
        etl.todb(table, conn, table_name, create=True, constraints=False)


def import_json(fil, index, conn, args):
    """
    Import json file data in sqlite database.
    The file must contain a JSON array as the top level object.
    Each member of the array will be treated as a row of data.
    """
    table = etl.fromjson(fil, lines=True)
    table_name = db.normalize_name(fil.with_suffix("").name, index)
    etl.todb(table, conn, table_name, create=True, constraints=False)


def import_ods(fil, index, conn, args):
    """
    Convert ods files to xlsx before importing because existing python ods readers are much too slow.
    """
    cmd = f"soffice --headless --convert-to xlsx {str(fil)} --outdir {str(args.tmp_dir)}"
    gui.print_msg("Converting '" + fil.name + "' to Excel format.", style=gui.style.info, highlight=True)
    exit_code, output = command_runner(cmd, timeout=180, encoding="utf-8")
    if exit_code == 0:
        import_excel(Path(args.tmp_dir, fil.with_suffix(".xlsx").name), index, conn, args)
    else:
        gui.print_msg(output, exit=True)


def import_files(args):
    """
    Import supported tabular files in source directory in sqlite database.
    """
    conn = sqlite3.connect(args.source_db_path)
    gui.print_msg("Creating sqlite database from files in directory...", style=gui.style.info)
    ext_funcs = ext_cmds()

    for index, fil in enumerate(args.data_files_dir.rglob("*")):
        if fil.suffix[1:].lower() not in ext_funcs.keys():
            continue

        ext_funcs[fil.suffix[1:].lower()](fil, index + 1, conn, args)


def export_xlsx(file_path, query, args):
    """
    Export db data from query to Excel file.
    """
    conn = sqlite3.connect(args.target_db_path)
    table = etl.fromdb(conn, query)
    etl.toxlsx(table, file_path)


def ensure_tables_from_files(req_tables, args):
    """
    Ensure tables are present in database after import from tabular data files.
    Takes into account modification of table names needed to ensure unique table names.
    """
    tables = {}
    for i in req_tables:
        tables[i] = None

    tables_count = configdb.get_tables_count(args.target, args)
    for db_table in tables_count.keys():
        for req_table in req_tables:
            if db_table.startswith(req_table + "_") and db_table[db_table.rindex("_") + 1:].isdigit():
                tables[req_table] = db_table

    for req_table, db_table in tables.items():
        if db_table is None:
            gui.print_msg("Missing table '" + req_table + "' in database. Aborted", exit=True, highlight=True)

    return tables
