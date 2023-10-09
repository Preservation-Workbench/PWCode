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
import subprocess

import sqlwb
import jdbc


def run(cfg):
    source = jdbc.get_conn(cfg.source, cfg)
    source_conn = sqlwb.get_connect_cmd(source, cfg)
    print(source_conn)

    # jdbc:sqlite:/home/pwb/bin/PWCode/projects/tester1/tmp/desktopsystemx-config.db
    # WbConnect -url=jdbc:sqlite:/home/pwb/bin/PWCode/projects/tester1/tmp/desktopsystemx-config.db -username= -password= -driverJar=/home/pwb/bin/PWCode/deps/jars/sqlite-jdbc.jar -driver=org.sqlite.JDBC;

    cmd = [
        cfg.java_bin,
        "-Djava.awt.headless=true",
        "-Dvisualvm.display.name=SQLWorkbench/J",
        "-cp",
        f'{str(cfg.sqlwb_bin)}:{str(Path(cfg.jars_dir,"ext"))}/*',
        "workbench.console.SQLConsole",
        f"-configDir={str(cfg.jars_dir)}",
        source_conn,
        # '-url=jdbc:h2:mem:PWB -password="";',
    ]

    # subprocess.call(cmd, universal_newlines=True)
