# Copyright(C) 2023 Morten Eek

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

import gui
import _copy
import _sqlite
from sqlite_utils import Database


def find_dupes(field, db):
    gui.print_msg(f"Finding {field} dupes...", style=gui.style.info)
    for row in db.query(f"""
            WITH bat_mod AS
            (
              SELECT user_id,
                     user_email,
                     first_name || ' ' || last_name AS first_last_name,
                     display_name
              FROM bataljonen
            )
            SELECT DISTINCT b.{field},
                   (SELECT group_concat(user_id)
                    FROM bat_mod
                    WHERE {field} = b.{field}) AS {field}_dupes,
                   cnt AS number
            FROM bat_mod b
              JOIN (SELECT {field},
                           COUNT(*) AS cnt
                    FROM bat_mod
                    GROUP BY {field}) c ON b.{field} = c.{field}
            WHERE c.cnt > 1
            ORDER BY b.{field};
            """):
        for user_id in row[f"{field}_dupes"].split(","):
            db["bataljonen"].update(user_id, {f"{field}_dupes": row[f"{field}_dupes"]})


def run(main_cfg):
    """
    Run from cli like this:
    On Linux: ./pwcode script --path scripts/bat_data.py
    """

    # COPY DATA:
    gui.print_msg("Importing data from files...", style=gui.style.info)
    main_cfg.source = "/home/pwb/Downloads/bataljonen/data"
    main_cfg.target = "bataljonen"
    cfg = _copy.run(main_cfg)

    # VALIDATE DATA:
    req_tables = ["wp_mepr_transactions", "wp_users", "wp_usermeta", "interesse", "partoutkort", "aktive_medlemmer"]
    tables = _sqlite.ensure_tables_from_files(req_tables, cfg)

    # TRANSFORM DATA:
    gui.print_msg("Transforming data...", style=gui.style.info)
    db = Database(cfg.target_db_path, use_counts_table=True)

    db["users"].create(
        {
            "user_email": str,
            "first_name": str,
            "last_name": str,
            "display_name": str,
            "user_id": int
        },
        if_not_exists=True,
    )

    db.executescript(f"""
        DELETE FROM users;
        INSERT INTO users
        SELECT u.user_email,
               firstmeta.meta_value AS first_name,
               lastmeta.meta_value AS last_name,
               u.display_name,
               u.ID
               FROM {tables["wp_users"]} u
          LEFT JOIN {tables["wp_usermeta"]} AS firstmeta
                 ON u.ID = firstmeta.user_id
                AND firstmeta.meta_key = 'first_name'
          LEFT JOIN {tables["wp_usermeta"]} AS lastmeta
                 ON u.ID = lastmeta.user_id
                AND lastmeta.meta_key = 'last_name';
        """)

    db["ansiennitet"].create(
        {
            "user_email": str,
            "first_name": str,
            "last_name": str,
            "display_name": str,
            "user_id": int,
            "year": int
        },
        if_not_exists=True,
    )

    # None with year null after 2018 so can ignore
    db.executescript(f"""
        DELETE FROM ansiennitet;
        INSERT INTO ansiennitet
        SELECT u.user_email,
               u.first_name,
               u.last_name,
               u.display_name,
               t.user_id,
               strftime('%Y',t.created_at) AS "year"
        FROM {tables["wp_mepr_transactions"]} t,
             users u
        WHERE t.user_id = u.user_id
        AND   t.status = 'complete'
        AND   year IS NOT NULL
        GROUP BY t.user_id,
                 year
        ORDER BY u.display_name DESC;
        """)

    db["bataljonen"].create(
        {
            "user_id": int,
            "user_email": str,
            "first_name": str,
            "last_name": str,
            "display_name": str,
            "years": str,
            "consecutive_years": int,
            "user_email_dupes": int,
            "display_name_dupes": int,
            "first_last_name_dupes": int,
        },
        pk="user_id",
        if_not_exists=True,
    )

    db.executescript("""
        DELETE FROM bataljonen;
        INSERT INTO bataljonen
        SELECT user_id,
               user_email,
               first_name,
               last_name,
               display_name,
               group_concat(year) AS years,
               0,
               0,
               0,
               0
        FROM ansiennitet
        GROUP BY user_id
        ORDER BY user_id;
        """)

    db["interesse"].create(
        {
            "id": int,
            "first_name": str,
            "last_name": str,
            "email": str,
            "phone": str,
            "user_id": str,
            "active_member": int,
            "duplicates": int,
        },
        pk="id",
        if_not_exists=True,
    )

    db.executescript(f"""
        DELETE FROM interesse;
        INSERT INTO interesse
        SELECT rowid,
               billing_first_name,
               billing_last_name,
               billing_e_mail,
               billing_phone,
               customer_id,
               0,
               0
        FROM {tables["interesse"]}
        """)

    # CALCULATE:
    gui.print_msg("Calculating consecutive years...", style=gui.style.info)
    year = 2022
    years = [str(i) for i in list(range(year, year - 21, -1))]  # 2022 -> 2002
    for row in db["bataljonen"].rows:
        user_years = [i for i in row["years"].split(",")]
        cnt = 0
        for year in years:
            if year in user_years:
                cnt += 1
            else:
                break

        db["bataljonen"].update(row["user_id"], {"consecutive_years": cnt})

    for field in ["user_email", "display_name", "first_last_name"]:
        find_dupes(field, db)

    gui.print_msg("Finding email dupes in interesse table...", style=gui.style.info)
    for row in db.query("""
            SELECT b.email,
                   b.rowid,
                   cnt AS number
            FROM interesse b
              JOIN (SELECT email, COUNT(*) AS cnt FROM interesse GROUP BY email) c ON b.email = c.email
            WHERE c.cnt > 1
            ORDER BY b.email;
            """):
        db["interesse"].update(row["id"], {"duplicates": row["number"]})

    gui.print_msg("Finding requests with active membership(1) and partoukort(2) ...", style=gui.style.info)
    for row in db.query(f"""
            WITH interesse_mod AS
            (
              SELECT rowid,
                     email
              FROM interesse
              WHERE email IN (SELECT email FROM {tables["aktive_medlemmer"]})
            )
            SELECT *,
                   CASE
                     WHEN email IN (SELECT billettkjoeper_e_post FROM {tables["partoutkort"]}) THEN 2
                     ELSE 1
                   END AS active_member
            FROM interesse_mod
            """):
        db["interesse"].update(row["rowid"], {"active_member": row["active_member"]})

    # EXPORT DATA:
    file_path = Path(Path.home(), "bataljonen.xlsx")
    gui.print_msg("Exporting data to '" + file_path.name + "' in home directory...",
                  style=gui.style.info,
                  highlight=True)
    _sqlite.export_xlsx(file_path, "SELECT * FROM bataljonen ORDER BY consecutive_years DESC", cfg)

    file_path = Path(Path.home(), "interesse.xlsx")
    gui.print_msg("Exporting data to '" + file_path.name + "' in home directory...",
                  style=gui.style.info,
                  highlight=True)
    _sqlite.export_xlsx(
        file_path, "SELECT first_name, last_name, email, phone, user_id, active_member, duplicates FROM interesse", cfg)

    sql = """
    WITH interesse_mod AS
    (
      SELECT MIN(rowid) AS id,
             email
      FROM interesse
      WHERE active_member = 1
      GROUP BY email
    )
    SELECT *
    FROM bataljonen
    WHERE user_email IN (SELECT email FROM interesse_mod)
    ORDER BY consecutive_years DESC;
    """
    file_path = Path(Path.home(), "ansiennitet_interesse1.xlsx")
    gui.print_msg("Exporting data to '" + file_path.name + "' in home directory...",
                  style=gui.style.info,
                  highlight=True)
    _sqlite.export_xlsx(file_path, sql, cfg)
