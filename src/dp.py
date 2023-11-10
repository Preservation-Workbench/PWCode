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

import json
from pathlib import Path

import gui
from frictionless import Package, Resource, platform
from typing import Type, Dict
import db
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import TypeEngine
from sqlalchemy.dialects.oracle import VARCHAR2
import configdb


def write_field(engine, field):
    """Convert frictionless field to sqlalchemy type
    as e.g. Field(type=string) -> sa.Text
    """

    sapg = platform.sqlalchemy_dialects_postgresql

    # Default dialect:
    mapping: Dict[str, Type[TypeEngine]] = {
        "any": sa.Text,
        "boolean": sa.Boolean,
        "date": sa.Date,
        "datetime": sa.DateTime,
        "integer": sa.Integer,
        "number": sa.Float,
        "string": sa.Text,
        "time": sa.Time,
        "year": sa.Integer,
    }

    # Postgresql dialect:
    if engine.dialect.name.startswith("postgresql"):
        mapping.update({
            "array": sapg.JSONB,
            "geojson": sapg.JSONB,
            "number": sa.Numeric,
            "object": sapg.JSONB,
        })

    type = mapping.get(field.type, sa.Text)
    return type


def write_table(engine, schema, fk, *, table_name):
    """Convert frictionless schema to sqlalchemy table"""
    columns = []
    constraints = []

    # Fields:
    Check = sa.CheckConstraint
    quote = engine.dialect.identifier_preparer.quote  # type: ignore
    for field in schema.fields:
        checks = []
        nullable = not field.required
        quoted_name = quote(field.name)
        column_type = write_field(engine, field)
        unique = field.constraints.get("unique", False)
        # https://stackoverflow.com/questions/1827063/mysql-error-key-specification-without-a-key-length
        if engine.dialect.name.startswith("mysql"):
            unique = unique and field.type != "string"
        for const, value in field.constraints.items():
            # if const == "minLength":
            # checks.append(Check("LENGTH(%s) >= %s" % (quoted_name, value)))
            if const == "maxLength":
                for prefix in ["mysql", "db2", "ibm", "sqlite", "postgresql"]:
                    if engine.dialect.name.startswith(prefix):
                        column_type = sa.VARCHAR(length=value)

                if engine.dialect.name.startswith("oracle"):
                    if value > 4000:
                        column_type = sa.CLOB()
                    else:
                        column_type = VARCHAR2(length=value)

                # checks.append(Check("LENGTH(%s) <= %s" % (quoted_name, value)))
            # elif const == "minimum":
            # checks.append(Check("%s >= %s" % (quoted_name, value)))
            # elif const == "maximum":
            # checks.append(Check("%s <= %s" % (quoted_name, value)))
            elif const == "pattern":
                if engine.dialect.name.startswith("postgresql"):
                    checks.append(Check("%s ~ '%s'" % (quoted_name, value)))
                else:
                    check = Check("%s REGEXP '%s'" % (quoted_name, value))
                    checks.append(check)
            elif const == "enum":
                # NOTE: https://github.com/frictionlessdata/frictionless-py/issues/778
                if field.type == "string":
                    enum_name = "%s_%s_enum" % (table_name, field.name)
                    column_type = sa.Enum(*value, name=enum_name)
        column_args = [field.name, column_type] + checks
        column_kwargs = {"nullable": nullable, "unique": unique, "autoincrement": False}
        if field.description:
            column_kwargs["comment"] = field.description
        column = sa.Column(*column_args, **column_kwargs)
        columns.append(column)

    if schema.primary_key:
        constraint = sa.PrimaryKeyConstraint(*schema.primary_key)
        constraints.append(constraint)

    if fk:
        for fk in schema.foreign_keys:
            fields = fk["fields"]
            foreign_fields = fk["reference"]["fields"]
            foreign_table_name = fk["reference"]["resource"] or table_name
            foreign_fields = list(map(lambda field: ".".join([foreign_table_name, field]), foreign_fields))
            constraint = sa.ForeignKeyConstraint(fields, foreign_fields)
            constraints.append(constraint)

    table = sa.Table(table_name, sa.MetaData(engine), *(columns + constraints))
    return table


def create_schema(cfg, changed, tables=[], schema_path=None):
    if schema_path is None:
        schema_path = Path(cfg.content_dir, "datapackage.json")

    if len(tables) == 0:
        target_name = cfg.target_name
    else:
        target_name = "partial"

    # import csv
    dialect = {
        "delimiter": "\t",
        "quoteChar": "\0",
        "escapeChar": "\0",
        "doubleQuote": False,
        "skipInitialSpace": False,
    }

    if schema_path.is_file() and cfg.stop != "json" and not changed and len(tables) == 0:
        gui.print_msg("Datapackage.json already generated.", style=gui.style.info)
        return schema_path

    if schema_path.is_file():
        schema_path.unlink()

    if len(tables) == 0:
        gui.print_msg("Generating datapackage.json...", style=gui.style.info)

    package = Package(
        name=target_name,
        profile="tabular-data-package",
        resources=[],
    )

    norm_tables = configdb.get_norm_tables(cfg.config_db)
    norm_columns = configdb.get_norm_columns(cfg.config_db)
    for row in cfg.config_db.query("""
            SELECT source_name,
                   norm_name,
                   source_pk,
                   source_row_count,
                   empty_rows,
                   deps
            FROM tables
            WHERE source_row_count > 0
            AND   include = 1
            ORDER BY deps_order ASC
            """):
        source_table = str(row["source_name"])
        norm_table = str(row["norm_name"])
        source_pk = str(row["source_pk"])
        deps = str(row["deps"])

        row_count = int(row["source_row_count"])
        empty_rows = int(row["empty_rows"])
        row_count = str(row_count - empty_rows)

        if len(tables) > 0 and source_table not in tables:
            continue

        pk = []
        fields = []
        for row in cfg.config_db.query(f"""
                SELECT source_column,
                       norm_column,
                       jdbc_data_type,
                       source_column_size
                FROM columns
                WHERE source_table = '{source_table}'
                """):
            source_column = str(row["source_column"])
            norm_column = str(row["norm_column"])
            jdbc_data_type = int(row["jdbc_data_type"])
            db_type = db.get_type("jdbc_no", "datapackage", jdbc_data_type)
            source_column_size = int(row["source_column_size"])
            field = {
                "name": norm_column,
                "type": db_type,
                "jdbc_type": str(jdbc_data_type),
                "db_column_name": source_column,
            }

            constr = {"constraints": {}}

            if jdbc_data_type in (-16, -15, -9, -8, -1, 1, 12, 2005, 2009, 2011):
                constr["constraints"]["maxLength"] = source_column_size

                # Don't apply constraint for empty columns (source_column_size = 0)
                # Potentially untrue for cases like long in oracle with undetectable size
                if source_column_size > 0 and not (jdbc_data_type == -1 and cfg.source.type == "oracle"):
                    field.update(constr)

            if source_column == source_pk:
                constr["constraints"]["required"] = True
                pk.append(norm_column)
                field.update(constr)

            fields.append(field)

        table_descr = {"fields": fields}
        if pk:
            table_descr.update({"primaryKey": pk})

        foreign_keys = []
        for row in cfg.config_db.query(f"""
                SELECT c.norm_column,
                       f.source_ref_table,
                       f.source_ref_column
                FROM foreign_keys f
                  LEFT JOIN columns c
                         ON c.source_column = f.source_column
                        AND c.source_table = f.source_table
                WHERE c.source_table = '{source_table}' AND f.is_enabled = True
                """):
            source_ref_table = str(row["source_ref_table"])
            if source_ref_table not in norm_tables:
                continue

            foreign_keys.append({
                "fields": str(row["norm_column"]),
                "reference": {
                    "resource": norm_tables[source_ref_table],
                    "fields": norm_columns[source_ref_table + ":" + str(row["source_ref_column"])],
                },
            })

        if foreign_keys:
            table_descr.update({"foreignKeys": foreign_keys})

        resource = Resource({
            "name": norm_table,
            "profile": "tabular-data-resource",
            "path": str(Path("data", norm_table + ".tsv")),
            "encoding": "UTF-8",
            "db_table_name": source_table,
            "db_table_deps": deps,
            "count_of_rows": str(row_count),
            "schema": table_descr,
            "dialect": dialect,
        })
        package.add_resource(resource)

    package.to_json(schema_path)

    if cfg.stop == "json":
        gui.show(cfg, schema_path)

    return schema_path


def create_ddl(schema_path, changed, cfg):
    jdbc = cfg.target
    ddl_file = Path(cfg.content_dir, jdbc.type + "-ddl.sql")
    ddl_fk_file = Path(cfg.content_dir, jdbc.type + "-fk-ddl.sql")
    files = [ddl_fk_file]

    if cfg.target.type != "sqlite":
        files.append(ddl_file)

    for fil in files:
        if fil.is_file() and cfg.stop != "ddl" and not changed:
            gui.print_msg("DDL for schema already generated.", style=gui.style.info)
        else:
            if not schema_path.is_file():
                gui.print_msg("JSON schema-file '" + str(schema_path) + "' missing. Aborted", exit=True)

            def _dump(sql, *multiparams, **params):
                pass

            gui.print_msg("Generating DDL from datapackage json schema...", style=gui.style.info)

            if jdbc.type == "h2":
                jdbc.type = "postgresql"

            if fil.is_file():
                fil.unlink()

            fk = False
            if fil == ddl_fk_file:
                fk = True

            tables = []
            meta = sa.MetaData()
            engine = sa.create_engine("%s://" % jdbc.type, strategy="mock", executor=_dump)
            with open(schema_path) as f:
                package = Package(json.load(f))
                for res in package.resources:
                    table = write_table(engine, res.schema, fk, table_name=res.name)
                    table = table.to_metadata(meta)
                    tables.append(table)

            with open(fil, "a") as f:
                for table in meta.sorted_tables:
                    tbl_ddl = CreateTable(table, if_not_exists=True).compile(engine)
                    lines = [line for line in str(tbl_ddl).splitlines()]
                    f.write("\n".join([line for line in lines if line.strip()]) + ";\n\n")

    if cfg.stop == "ddl":
        gui.show(cfg, ddl_fk_file)

    if cfg.target.type == "sqlite":
        return ddl_fk_file

    return ddl_file
