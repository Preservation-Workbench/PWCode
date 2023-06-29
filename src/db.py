# MIT License

# Original work Copyright (c) 2020 Open Knowledge Foundation
# Modified work Copyright 2023 Morten Eek

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import sys
import os

import petl as etl
import gui
import configdb
from sqlite_utils import Database
from functools import reduce


def fix_fk(cfg):
    fixed = False

    if cfg.target.type == "sqlite":
        db = Database(cfg.target_db_path)

    norm_tables = configdb.get_norm_tables(cfg.config_db)
    norm_columns = configdb.get_norm_columns(cfg.config_db)
    for row in cfg.config_db.query(
        f"""
        SELECT source_name,
               source_table,
               source_ref_table,
               source_column,
               source_ref_column
        FROM foreign_keys
        WHERE IFNULL(target_name,'') = ''
        """
    ):
        if all(x in norm_tables for x in [row["source_table"], row["source_ref_table"]]):
            fixed = True
            source_table = row["source_table"]
            source_ref_table = row["source_ref_table"]
            norm_table = norm_tables[source_table]
            norm_ref_table = norm_tables[source_ref_table]
            norm_column = norm_columns[source_table + ":" + row["source_column"]]
            norm_ref_column = norm_columns[source_ref_table + ":" + row["source_ref_column"]]

            gui.print_msg(
                "Adding foreign key to table "
                + norm_table
                + " ("
                + norm_column
                + " references "
                + norm_ref_table
                + "."
                + norm_ref_column
                + ")",
                style="bold cyan",
            )

            if cfg.target.type == "sqlite":
                db[norm_table].add_foreign_key(norm_column, norm_ref_table, norm_ref_column, ignore=True)
            else:
                sql = (
                    '\nALTER TABLE "'
                    + norm_table
                    + '"'
                    + ' ADD CONSTRAINT "'
                    + str(row["source_name"])
                    + '" FOREIGN KEY ('
                    + norm_column
                    + ") "
                    + norm_ref_table
                    + " ("
                    + norm_ref_column
                    + ");"
                )

                result = jdbc.run_command(cfg.target, sql, cfg)
                print(result)

    if fixed and cfg.target.type == "sqlite":
        db.index_foreign_keys()  # Add indexes to any foreign keys without


def get_type(key_column, value_column, Key_value):
    db_types = [
        ["jdbc_no", "jdbc_name", "iso", "sqlite", "postgresql", "oracle", "datapackage"],
        [-16, "longnvarchar", "clob", "clob", "text", "clob", "string"],
        [-15, "nchar", "varchar()", "varchar()", "varchar()", "varchar()", "string"],
        [-9, "nvarchar", "varchar()", "varchar()", "varchar()", "varchar()", "string"],
        [-8, "rowid", "varchar()", "varchar()", "varchar()", "varchar()", "string"],
        [-7, "bit", "boolean", "boolean", "boolean", "integer", "integer"],
        [-6, "tinyint", "integer", "integer", "integer", "integer", "integer"],
        [-5, "bigint", "bigint", "bigint", "bigint", "number", "integer"],
        [-4, "longvarbinary", "blob", "blob", "bytea", "blob", "string"],
        [-3, "varbinary", "blob", "blob", "bytea", "blob", "string"],
        [-2, "binary", "blob", "blob", "bytea", "blob", "string"],
        [-1, "longvarchar", "clob", "clob", "text", "clob", "string"],
        [1, "char", "varchar()", "varchar()", "varchar()", "varchar()", "string"],
        [2, "numeric", "numeric", "numeric", "numeric", "numeric", "number"],
        [3, "decimal", "decimal", "decimal", "decimal", "decimal", "number"],
        [4, "integer", "integer", "integer", "integer", "integer", "integer"],
        [5, "smallint", "integer", "integer", "integer", "integer", "integer"],
        [6, "float", "float", "float", "float", "number", "number"],
        [7, "real", "real", "real", "real", "real", "number"],
        [8, "double", "double precision", "double precision", "double precision", "double precision", "number"],
        [12, "varchar", "varchar()", "varchar()", "varchar()", "varchar()", "string"],
        [16, "boolean", "boolean", "boolean", "boolean", "integer", "boolean"],
        [91, "date", "date", "date", "date", "date", "date"],
        [92, "time", "time", "time", "time", "date", "time"],
        [93, "timestamp", "timestamp", "timestamp", "timestamp", "timestamp", "datetime"],
        [2004, "blob", "blob", "blob", "bytea", "blob", "string"],
        [2005, "clob", "clob", "clob", "text", "clob", "string"],
        [2009, "SQLXML", "clob", "clob", "text", "clob", "string"],
        [2011, "nclob", "clob", "clob", "text", "clob", "string"],
    ]

    return etl.lookup(db_types, key_column, value_column)[Key_value][0]


def normalize_name(name, index, length=False):
    repls = (
        (" ", "_"),
        ("-", "_"),
        ("æ", "ae"),
        ("ø", "oe"),
        ("å", "aa"),
    )

    name = reduce(lambda a, kv: a.replace(*kv), repls, name.lower())
    if len(name) > 30 or length:
        name = name[:25] + "_" + str(index)

    return name
