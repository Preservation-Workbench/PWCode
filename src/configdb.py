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
from sqlite_utils.db import NotFoundError
from toposort import toposort_flatten
from pathlib import Path
import json


@dataclass
class SubSystem:
    name: str
    source_schema: str
    target_schema: str
    source_type: str
    target_type: str
    args: str


def create_db(path):
    """
    Create config database
    """
    configdb = Database(path, use_counts_table=True)
    configdb.enable_wal()

    configdb["sub_systems"].create(
        {
            "name": str,  # Name of directory under content in project
            "source_schema": str,  # Schema name in source database
            "target_schema": str,  # Schema name in target database
            "source_type": str,  # Source database type
            "target_type": str,  # Target database type
            "args": str,  # Arguments pwcode was run with
        },
        pk="name",
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
            "empty_rows": int,  # No of completely enpty rows
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
            "empty_rows": 0,
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
            "is_enabled": bool,
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


def has_cycle(graph):
    """
    Detects circular dependencies in a graph represented as a dictionary.

    Args:
        graph (dict): A dictionary where keys are nodes and values are lists of their dependencies.

    Returns:
        bool: True if the graph has a circular dependency, False otherwise.

    Raises:
        None

    Algorithm:
    This function uses depth-first search (DFS) to explore the graph. During the traversal,
    if a node is encountered that is already in the current recursion stack, it indicates a cycle,
    and the function returns True. If no cycles are found after the traversal, the function returns False.
    """

    def dfs(node, visited, recursion_stack):
        visited[node] = True
        recursion_stack[node] = True

        # Visit all neighbors
        for neighbor in graph.get(node, []):
            if neighbor == node:  # Skip self-references
                continue
            if not visited[neighbor]:
                if dfs(neighbor, visited, recursion_stack):
                    return True
            elif recursion_stack[neighbor]:
                return True

        # Remove the node from the recursion stack after exploration
        recursion_stack[node] = False
        return False

    # Dictionary to keep track of visited nodes
    visited = {node: False for node in graph}

    # Dictionary to keep track of nodes in the current DFS recursion stack
    recursion_stack = {node: False for node in graph}

    # Check for cycles using DFS
    for node in graph:
        if not visited[node]:
            if dfs(node, visited, recursion_stack):
                return True  # Found a cycle

    return False  # No cycle found


def update_table_deps(tables, cfg):
    """
    Write dependent tables per table to config database
    """

    deps_file = Path(cfg.tmp_dir, cfg.target_name + "-deps.json")
    deps_dict = {}

    # Check if we have a json file containing dependencies
    if deps_file.exists():
        # Read from file
        gui.print_msg("Reading dependencies from file...", style=gui.style.info)
        with open(deps_file, 'r', encoding='utf-8') as file:
            deps_dict = json.load(file)

    else:
        # Nope, no file. Create new dependency map.
        gui.print_msg("Get dependencies per table...", style=gui.style.info)
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

    if has_cycle(deps_dict):
        with open(deps_file, 'w', encoding='utf-8') as file:
            json.dump(deps_dict, file, ensure_ascii=False, indent=4)
        gui.print_msg("Cyclic dependencies detected, dependencies written to '" + str(deps_file) +
                      "'. Aborting, please review this file and then re-run the program.",
                      exit=True)

    # Update 'foreign_keys' table and enable the active constraints.
    r = 0
    for dep, val in deps_dict.items():
        if dep and val:
            for v in val:
                r += 1
                cfg.config_db.execute(f"""
                    UPDATE foreign_keys
                    SET is_enabled = True
                    WHERE source_table = '{dep}' AND source_ref_table = '{v}'
                """)
    print(f"{r} constraint(s) enabled.")

    # Update 'tables' table with dependencies.
    sorted_tables = toposort_flatten(deps_dict)
    order = 0
    for table in sorted_tables:
        order += 1
        cfg.config_db["tables"].update(table, {"deps": ",".join(deps_dict[table]), "deps_order": order})


def connect_column_fk(cfg):
    """
    Connect foreign key references to table-column-postions
    """
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
    """
    Retrieve table names to normalized table names mapping
    """
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


def get_sub_system(system, config_db):
    """
    Retrieve informaton about original sub_system source before running archive command
    """
    try:
        sub_system = config_db["sub_systems"].get(system)
    except NotFoundError:
        return

    return SubSystem(**sub_system)


def update_sub_system(cfg):
    """
    Update information about sub_system
    """
    sub_system = get_sub_system(cfg.content_dir.name, cfg.config_db)
    if sub_system is None:
        cfg.config_db["sub_systems"].insert(
            {
                "name": cfg.content_dir.name,
                "source_schema": cfg.source.schema,
                "target_schema": cfg.target.schema,
                "source_type": cfg.source.type,
                "target_type": cfg.target.type,
                "args": cfg.args,
            },
            pk="name",
        )
    elif "--stop" not in cfg.args:
        cfg.config_db["sub_systems"].update(cfg.content_dir.name, {"args": cfg.args})


def get_norm_columns(config_db):
    """
    Retrieve column names to normalized column names mapping
    """
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
    """
    Retrieve list of tables to be copied
    """
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


def get_copied_tables(cfg):
    """
    Retrieve list of tables already copied
    """
    copied_tables = []
    for row in cfg.config_db.query("""
            SELECT source_name
            FROM tables
            WHERE source_row_count > 0
            AND   source_row_count = target_row_count
            """):
        copied_tables.append(row["source_name"])

    return copied_tables


def get_validated_tables(cfg):
    """
    Retrieve list of tables already copied + validated
    """
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
    """
    Retrieve row count per table for all tables
    """
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
    """
    Modify list of tables to be copied
    """
    for row in cfg.config_db["tables"].rows:
        if row["source_name"] in tables or (int(row["source_row_count"]) > 0
                                            and int(row["target_row_count"]) == int(row["source_row_count"])):
            cfg.config_db["tables"].update(row["source_name"], {"include": 1})

        if row["source_name"] not in tables and (int(row["target_row_count"]) != int(row["source_row_count"])
                                                 or int(row["source_row_count"]) == 0):
            cfg.config_db["tables"].update(row["source_name"], {"include": 0})


def get_tables_deps(cfg):
    """
    Retrieve dependent tables per table for all tables
    """
    deps_pr_table = {}
    for row in cfg.config_db.query("""
            SELECT source_name,
                   deps
            FROM tables
            """):
        deps_pr_table[row["source_name"]] = row["deps"]

    return deps_pr_table


def get_cp_error_tables(cfg):
    """
    Retrieve list of tables where errors occurred during copying
    """
    error_tables = []
    for row in cfg.config_db["tables"].rows_where("cp_error = 1"):
        error_tables.append(row["source_name"])

    return error_tables


def tables_diff(cfg):
    """
    Retrieve list of tables not yet created in target database
    """
    created_tables = []
    for row in cfg.config_db["tables"].rows_where("created = 1"):
        created_tables.append(row["source_name"])

    diff_tables = []
    for table in get_include_tables(cfg):
        if table not in created_tables:
            diff_tables.append(table)

    return diff_tables


def data_diff(cfg, count=False):
    """
    Retrieve list of tables in target database with missing data compared to source database
    """
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
