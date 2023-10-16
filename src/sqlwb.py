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
import json
import tempfile

import jpype as jp
from frictionless import Package
from utils import _dict
import jdbc
import gui
from sqlalchemy import create_engine
import configdb


def get_copy_statements(json_schema_file, cfg, diff_data):

    def _dump(sql, *multiparams, **params):
        pass

    copy_file = Path(cfg.tmp_dir, cfg.target_name + "-copy.sql")
    if copy_file.is_file() and cfg.stop != "copy" and not diff_data:
        gui.print_msg("Copy statements already generated.", style=gui.style.info)
    else:
        if not json_schema_file.is_file():
            gui.print_msg("Datapackage json schema '" + str(json_schema_file) + "' missing. Aborted", exit=True)

        gui.print_msg("Generating copy data statements...", style=gui.style.info)

        if copy_file.is_file():
            copy_file.unlink()

        # params = "-mode=INSERT -ignoreIdentityColumns=false -commitEvery=10000 "
        params = "-mode=INSERT -ignoreIdentityColumns=false "  # Bug in -commitEvery - at least for h2
        pragmas = ";".join((
            "PRAGMA foreign_keys=0",
            "PRAGMA journal_mode=0",
            "PRAGMA synchronous=0",
            "PRAGMA temp_store=MEMORY",
        ))

        url = cfg.target.short_url
        if cfg.target.type == "sqlite":
            url = (url + ",driverJar=" + cfg.jdbc_drivers[cfg.target.type]["jar"] + ",driver=" +
                   cfg.jdbc_drivers[cfg.target.type]["class"])
            params = params + '-preTableStatement="' + pragmas + '" '

        source_type = cfg.source.type.replace("h2", "postgresql")
        target_type = cfg.target.type.replace("h2", "postgresql")

        source_quote = create_engine("%s://" % source_type, strategy="mock",
                                     executor=_dump).dialect.identifier_preparer.quote

        target_quote = create_engine("%s://" % target_type, strategy="mock",
                                     executor=_dump).dialect.identifier_preparer.quote

        with open(json_schema_file) as f:
            package = Package(json.load(f))

            for table in package.resources:
                source_table_name = table.custom["db_table_name"]
                if cfg.schema:
                    source_table_name = cfg.schema + "." + source_table_name

                target_table_name = target_quote(table.name)

                ddl_columns = []
                for field in table.schema.fields:
                    source_column_name = field.custom["db_column_name"]
                    target_column_name = field.name
                    jdbc_db_type = int(field.custom["jdbc_type"])
                    fixed_source_column_name = ""

                    if cfg.no_blobs and jdbc_db_type in [-4, -3, -2, 2004]:
                        fixed_source_column_name = ("NULL AS " + source_quote(target_column_name) + ",")
                    elif jdbc_db_type in [91, 93] and cfg.target.type == "sqlite":
                        if cfg.source.type == "h2":
                            fixed_source_column_name = ("FORMATDATETIME(" + source_quote(source_column_name) +
                                                        ",'YYYY-MM-DD HH:mm:ss') AS " +
                                                        source_quote(target_column_name) + ",")
                        elif cfg.source.type == "sqlite":
                            fixed_source_column_name = ("DATETIME(SUBSTR(" + source_quote(source_column_name) +
                                                        ",1,10), 'unixepoch') AS " + source_quote(target_column_name) +
                                                        ",")
                        elif cfg.source.type == "oracle":
                            fixed_source_column_name = ("TO_CHAR(" + source_quote(source_column_name) +
                                                        ",'YYYY-MM-DD HH24:MM:SS') AS " +
                                                        source_quote(target_column_name) + ",")
                        else:
                            gui.print_msg(
                                "Datetime to formatted string in sqlite not implemented for '" + cfg.source.type + "'",
                                exit=True,
                            )
                    elif jdbc_db_type == 92 and cfg.target.type == "sqlite":
                        if cfg.source.type == "h2":
                            fixed_source_column_name = ("FORMATDATETIME(" + source_quote(source_column_name) +
                                                        ",'HH:mm:ss') AS " + source_quote(target_column_name) + ",")
                        elif cfg.source.type == "sqlite":
                            fixed_source_column_name = ("TIME(" + source_quote(source_column_name) + ") AS " +
                                                        source_quote(target_column_name) + ",")
                        elif cfg.source.type == "oracle":
                            fixed_source_column_name = ("TO_CHAR(" + source_quote(source_column_name) +
                                                        ",'HH24:MM:SS') AS " + source_quote(target_column_name) + ",")
                        else:
                            gui.print_msg(
                                "Time to formatted string in sqlite not implemented for '" + cfg.source.type + "'",
                                exit=True,
                            )

                    elif source_column_name.lower() == target_column_name.lower():
                        fixed_source_column_name = source_quote(source_column_name) + ","
                    else:
                        fixed_source_column_name = (source_quote(source_column_name) + " AS " +
                                                    source_quote(target_column_name) + ",")

                    ddl_columns.append(fixed_source_column_name)

                source_query = " ".join((
                    "SELECT",
                    "".join(ddl_columns)[:-1],
                    "FROM",
                    source_table_name,
                ))

                copy_data_str = ("WbCopy " + params + '-targetConnection="username=' + cfg.target.user + ",password=" +
                                 cfg.target.password + ",url=" + url + '" -targetTable="' + cfg.target.schema + '".' +
                                 target_table_name + " -sourceQuery=" + source_query + ";")

                with open(copy_file, "a") as file:
                    file.write("\n" + copy_data_str)

    if cfg.stop == "copy":
        gui.show(cfg, copy_file)

    return copy_file


def run_ddl_file(jdbc, cfg, diff_tables, ddl_file, echo=False):
    if not ddl_file.is_file():
        gui.print_msg("SQL file '" + str(ddl_file) + "' missing. Aborted", exit=True)

    gui.print_msg("Creating tables from generated DDL...", style=gui.style.info)

    result = None
    norm_tables = configdb.get_norm_tables(cfg.config_db)
    conn = get_connect_cmd(jdbc, cfg)
    batch = get_batch()

    with tempfile.TemporaryDirectory() as td:
        with open(ddl_file) as fr:
            for tbl_ddl in fr.read().split(";"):
                norm_table = tbl_ddl.partition("CREATE TABLE IF NOT EXISTS ")[2].partition(" (")[0]
                if norm_table in norm_tables.values():
                    source_table = _dict.get_key_from_value(norm_tables, norm_table)
                    if source_table not in diff_tables:
                        continue

                    with open(Path(td, norm_table + "_ddl.sql"), "w") as fw:
                        fw.write(tbl_ddl)

                    gui.print_msg("Creating table '" + source_table + "':", style=gui.style.info, highlight=True)

                    result = str(
                        batch.runScript(" ".join((
                            conn,
                            "WbInclude",
                            "-file=" + str(ddl_file),
                            "-verbose=" + str(echo),
                            "-printStatements=" + str(cfg.debug),
                            "-encoding=UTF8;",
                            "commit; WbDisconnect;",
                        ))))

                    if str(result) == "Error":
                        gui.print_msg(str(result), exit=True)
                    else:
                        cfg.config_db.execute("update tables set created = 1 where norm_name = '" + norm_table + "';")


def get_connect_cmd(jdbc, cfg):
    connect_cmd = " ".join((
        "WbConnect -url=" + jdbc.short_url,
        "-username=" + jdbc.user,
        "-password=" + jdbc.password,
        "-driverJar=" + cfg.jdbc_drivers[jdbc.type]["jar"],
        "-driver=" + cfg.jdbc_drivers[jdbc.type]["class"] + ";",
    ))

    return connect_cmd


def get_batch():
    WbManager = jp.JPackage("workbench").WbManager
    WbManager.prepareForEmbedded()
    batch = jp.JPackage("workbench.sql").BatchRunner()
    batch.setAbortOnError(True)

    return jp.JPackage("workbench.sql").BatchRunner()


def sqlwb_truncate_table(target_table, source_table, cfg):
    batch = get_batch()
    target_conn = get_connect_cmd(cfg.target, cfg)
    if cfg.target.type in ["oracle", "postgresql", "mysql", "mssql", "h2"]:
        delete_cmd = target_conn + "TRUNCATE TABLE " + target_table + ";"
    else:
        delete_cmd = target_conn + "DELETE FROM " + target_table + ";"

    base_msg = "Error."
    if cfg.test:
        base_msg = "Test run."

    gui.print_msg(
        base_msg + " Deleting copied table '" + source_table + "' and referring tables:",
        style=gui.style.info,
        highlight=True,
    )

    del_result = str(batch.runScript(delete_cmd + "COMMIT; WbDisconnect;"))
    if del_result == "Error":
        cfg.config_db["tables"].update(source_table, {"del_error": 1})
    else:
        cfg.config_db["tables"].update(source_table, {"del_error": 0})


def run_command(jdbc, sql, cfg):
    sql = sql.strip()
    sql = sql[:-1:] + sql[-1].replace(";", "") + ";"

    cmd = " ".join((
        get_connect_cmd(jdbc, cfg),
        sql,
        "WbDisconnect;",
    ))

    if cfg.debug:
        print(sql)

    batch = get_batch()
    return str(batch.runScript(cmd))


def run_copy_file(cfg, copy_file, diff_data):
    if not copy_file.is_file():
        gui.print_msg("Copy statements file '" + str(copy_file) + "' missing. Aborted", exit=True)

    row_count_pr_table = configdb.get_tables_count(cfg.source, cfg)
    deps_pr_table = configdb.get_tables_deps(cfg)
    source = cfg.source
    target = cfg.target
    source_conn = get_connect_cmd(source, cfg)
    cp_result = ""

    statements = []
    with open(copy_file) as file:
        statements = [line for line in file.read().splitlines() if line.strip()]

    gui.print_msg("Copying tables from source to target database:\n", style=gui.style.info)

    imported_tables = []
    error_tables = []
    old_error_tables = configdb.get_cp_error_tables(cfg)

    for statement in statements:
        target_table = statement.partition("-targetTable=")[2].partition(" ")[0].partition(".")[2].strip()
        source_table = statement[statement.rindex(" ") + 1:][:-1].replace('"', "")
        if "." in source_table:
            source_table = source_table.rsplit(".")[1]

        source_row_count = int(row_count_pr_table[source_table])

        cp_result = ""
        if source_table not in diff_data:
            imported_tables.append(source_table)
            gui.print_msg("'" + source_table + "' already copied.", style=gui.style.info, highlight=True)

        elif not cfg.test or (cfg.test and old_error_tables and source_table in old_error_tables):
            gui.print_msg(
                "Copying " + str(source_row_count) + " rows from '" + source_table + "':",
                style=gui.style.info,
                highlight=True,
            )

            copy_cmd = " ".join((
                source_conn,
                statement,
                "WbDisconnect;",
            ))

            batch = get_batch()
            cp_result = str(batch.runScript(copy_cmd))

        target_row_count = jdbc.get_table_count(target, target_table, cfg)
        if cp_result == "Error" or (target_row_count != source_row_count):
            sqlwb_truncate_table(target_table, source_table, cfg)
            cfg.config_db["tables"].update(source_table, {"cp_error": 1})
            error_tables.append(source_table)
        else:
            cfg.config_db["tables"].update(
                source_table,
                {
                    "target_row_count": target_row_count,
                    "cp_error": 0,
                    "del_error": 0,
                    "include": 1,
                    "created": 1,
                },
            )
            imported_tables.append(source_table)

        if cfg.test:
            if old_error_tables and source_table in old_error_tables:
                delete_tables = deps_pr_table[source_table].split(",")
                if all(item in imported_tables for item in delete_tables):
                    for target_table in delete_tables:
                        sqlwb_truncate_table(target_table, source_table, cfg)

            else:
                gui.print_msg("'" + source_table + "' copied/deleted in previous test run.",
                              style=gui.style.info,
                              highlight=True)

    if error_tables:
        gui.print_msg("Errors on copying tables '" + ", ".join(error_tables) + "'", exit=True)

    return cp_result


def export_text_columns(dbo, table, text_columns, tsv_path, cfg):
    cmd = " ".join((
        get_connect_cmd(dbo, cfg),
        "WbExport",
        "-type=text",
        "-file=" + str(tsv_path),
        "-continueOnError=false",
        "-encoding=UTF8",
        "-header=true",
        "-decimal='.'",
        "-maxDigits=0",
        "-lineEnding=lf",
        "-replaceExpression='(\\n|\\r\\n|\\r|\\t)' -replaceWith=' '",
        "-trimCharData=true",
        "-nullString=''",
        "-showProgress=100000;",
        "SELECT " + ",".join(text_columns.values()) + " FROM " + table.name + ";",
        "WbDisconnect;",
    ))

    batch = get_batch()

    return str(batch.runScript(cmd))
