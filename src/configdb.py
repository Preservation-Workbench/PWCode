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

from dataclasses import dataclass

import gui
import jdbc
from sqlite_utils import Database
from toposort import toposort_flatten


@dataclass
class Schema:
    system: str
    source_schema: str
    target_schema: str
    source_type: str
    target_type: str


def create_db(path):
    configdb = Database(path, use_counts_table=True)
    configdb.enable_wal()

    configdb["schemas"].create(
        {
            "system": str,
            "source_schema": str,
            "target_schema": str,
            "source_type": str,
            "target_type": str,
        },
        pk=("system"),
        if_not_exists=True,
    )

    configdb["tables"].create(
        {
            "source_name": str,  # Table name in source schema
            "norm_name": str,  # Normalized and lower case version of source_name
            "target_name": str,  # Table name in target schema (normalized version of source_name)
            "source_row_count": int,
            "target_row_count": int,
            "source_pk": str,
            "target_pk": str,
            "deps": str,  # List of tables dependent on table
            "deps_order": int,  # Create tables according to this order (lowest number first)
            "cp_error": int,  # Error on copy operation if == 1
            "del_error": int,  # Error on delete data/truncate if == 1
            "include": int,  # Include in list of tables to sync
            "created": int,  # Target table created if == 1
            "validated": int,  # Exported as tsv and validated against datapackage schema
        },
        pk="source_name",
        defaults={
            "source_row_count": 0,
            "target_row_count": 0,
            "cp_error": 0,
            "del_error": 0,
            "include": 0,
            "created": 0,
            "validated": 0,
        },
        if_not_exists=True,
    )

    configdb["columns"].create(
        {
            "tbl_col_pos": str,
            "source_table": str,
            "source_column": str,  # Column name in source schema
            "norm_column": str,  # Normalized and lower case version of source_column
            "target_column": str,  # Column name in target schema (normalized version of source_column)
            "jdbc_data_type": int,
            "source_data_type": str,
            "target_data_type": str,
            "source_column_size": int,
            "target_column_size": int,
            "fixed_size": int,  # fix_column_size done for column if 1
            "source_column_nullable": int,
            "target_column_nullable": int,
            "source_column_position": int,
            "target_column_position": int,
            "source_column_autoincrement": str,
            "target_column_autoincrement": str,
            "source_column_default": int,
            "target_column_default": int,
        },
        pk="tbl_col_pos",
        defaults={
            "fixed_size": 0,
        },
        foreign_keys=[("source_table", "tables", "source_name")],  # source_table references tables.source_name
        if_not_exists=True,
    )

    configdb["foreign_keys"].create(
        {
            "source_name": str,
            "target_name": str,
            "tbl_col_pos": str,
            "ref_tbl_col_pos": str,
            "source_table": str,
            "target_table": str,
            "source_column": str,
            "target_column": str,
            "source_ref_table": str,
            "target_ref_table": str,
            "source_ref_column": str,
            "target_ref_column": str,
        },
        pk="source_name",
        foreign_keys=[
            ("source_table", "tables", "source_name"),  # source_table references tables.source_name
            ("tbl_col_pos", "columns", "tbl_col_pos"),  # tbl_col_pos references columns.tbl_col_pos
            ("ref_tbl_col_pos", "columns", "tbl_col_pos"),  # ref_tbl_col_pos references columns.tbl_col_pos
        ],
        if_not_exists=True,
    )

    configdb["files"].create(
        {
            "source_path": str,
            "tar_path": str,
            "tar_checksum": str,
            "tar_mtime": str,
            "tar_status": str,
        },
        pk="source_path",
        if_not_exists=True,
    )

    return configdb


def update_table_deps(tables, cfg):
    gui.print_msg("Get dependencies per table...", style=gui.style.info)

    deps_dict = {}
    for table in tables:
        table_deps = set()
        for row in cfg.config_db.query(f"""
                SELECT c.source_column,
                       f.source_ref_column,
                       f.source_ref_table
                FROM foreign_keys f
                  LEFT JOIN columns c
                         ON c.source_column = f.source_column
                        AND c.source_table = f.source_table
                WHERE c.source_table = '{table}'
                """):
            ref_table = row["source_ref_table"]
            if ref_table in tables:
                table_deps.add(ref_table)

        if len(table_deps) == 0:
            table_deps.add(table)

        deps_dict[table] = list(table_deps)

    sorted_tables = toposort_flatten(deps_dict)
    order = 0
    for table in sorted_tables:
        order += 1
        cfg.config_db["tables"].update(table, {"deps": ",".join(deps_dict[table]), "deps_order": order})


def connect_column_fk(cfg):
    # Connect foreign key references to table-column-postions:
    for row in cfg.config_db.query("""
            SELECT f.source_name,
                   f.source_table,
                   f.source_column,
                   f.source_ref_table,
                   f.source_ref_column,
                   (SELECT f.tbl_col_pos
                    FROM columns c
                    WHERE c.source_column = f.source_column
                    AND   c.source_table = f.source_table) AS tbl_col_pos,
                   (SELECT f.tbl_col_pos
                    FROM columns c
                    WHERE c.source_column = f.source_ref_column
                    AND   c.source_table = f.source_ref_table) AS ref_tbl_col_pos
            FROM foreign_keys f
            """):
        cfg.config_db["foreign_keys"].update(row["source_name"], {
            "tbl_col_pos": row["tbl_col_pos"],
            "ref_tbl_col_pos": row["ref_tbl_col_pos"]
        })


def get_norm_tables(config_db):
    norm_tables = {}
    for row in config_db.query("""
            SELECT source_name,
                   norm_name
            FROM tables
            WHERE source_row_count > 0
            AND   include = 1
            """):
        norm_tables[row["source_name"]] = row["norm_name"]

    return norm_tables


def get_schema_info(system, config_db):
    schema_info = config_db["schemas"].get(system)

    return Schema(**schema_info)


def get_norm_columns(config_db):
    norm_columns = {}
    for row in config_db.query("""
            SELECT c.source_table,
                   c.source_column,
                   c.norm_column
            FROM tables t
              inner JOIN columns c
                      ON c.source_table = t.source_name
                     AND t.source_row_count > 0
                     AND t.include = 1
            """):
        norm_columns[row["source_table"] + ":" + row["source_column"]] = row["norm_column"]

    return norm_columns


def get_include_tables(cfg):
    include_tables = []
    for row in cfg.config_db.query("""
            SELECT source_name
            FROM tables
            WHERE source_row_count > 0
            AND   include = 1
            ORDER BY deps_order ASC
            """):
        include_tables.append(row["source_name"])

    return include_tables


def get_validated_tables(cfg):
    validated_tables = []
    for row in cfg.config_db.query("""
            SELECT norm_name
            FROM tables
            WHERE source_row_count > 0
            AND   validated = 1
            ORDER BY deps_order ASC
            """):
        validated_tables.append(row["norm_name"])

    return validated_tables


def get_tables_count(jdbc, cfg):
    tables_count = {}
    if jdbc == cfg.source:
        for row in cfg.config_db.query("""
                SELECT source_name,
                       source_row_count
                FROM tables
                WHERE source_row_count > 0
                """):
            tables_count[row["source_name"]] = row["source_row_count"]
    else:
        for row in cfg.config_db.query("""
                SELECT source_name,
                       target_row_count
                FROM tables
                WHERE include = 1
                """):
            tables_count[row["source_name"]] = row["target_row_count"]

    return tables_count


def update_include(cfg, tables):
    for row in cfg.config_db["tables"].rows:
        if row["source_name"] in tables or (int(row["source_row_count"]) > 0
                                            and int(row["target_row_count"]) == int(row["source_row_count"])):
            cfg.config_db["tables"].update(row["source_name"], {"include": 1})

        if row["source_name"] not in tables and (int(row["target_row_count"]) != int(row["source_row_count"])
                                                 or int(row["source_row_count"]) == 0):
            cfg.config_db["tables"].update(row["source_name"], {"include": 0})


def get_copied_tables(cfg):
    copied_tables = []
    for row in cfg.config_db.query("""
            SELECT source_name
            FROM tables
            WHERE source_row_count > 0
            AND   source_row_count = target_row_count
            """):
        copied_tables.append(row["source_name"])

    return copied_tables


def get_tables_deps(cfg):
    deps_pr_table = {}
    for row in cfg.config_db.query("""
            SELECT source_name,
                   deps
            FROM tables
            """):
        deps_pr_table[row["source_name"]] = row["deps"]

    return deps_pr_table


def get_cp_error_tables(cfg):
    error_tables = []
    for row in cfg.config_db["tables"].rows_where("cp_error = 1"):
        error_tables.append(row["source_name"])

    return error_tables


def tables_diff(cfg):
    created_tables = []
    for row in cfg.config_db["tables"].rows_where("created = 1"):
        created_tables.append(row["source_name"])

    diff_tables = []
    for table in get_include_tables(cfg):
        if table not in created_tables:
            diff_tables.append(table)

    return diff_tables


def data_diff(cfg, count=False):
    diff_data = {}
    if cfg.test:
        return diff_data

    if count:
        jdbc.get_all_tables_count(cfg.target, cfg)

    tables = get_include_tables(cfg)
    for row in cfg.config_db.query("""
            SELECT source_name,
                   source_row_count
            FROM tables
            WHERE source_row_count != target_row_count
            """):
        if row["source_name"] in tables:
            diff_data[row["source_name"]] = row["source_row_count"]

    return diff_data
