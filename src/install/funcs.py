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

import os
import sys
import shutil
import subprocess
from pathlib import Path
import jdk
import gui
from maven_artifact import Downloader
from maven_artifact.artifact import Artifact
import requests

# TODO: Install https://github.com/mvdan/sh/releases


def _jdk(cfg):
    cfg.deps_java_dir.mkdir(parents=True, exist_ok=True)
    Path(cfg.deps_java_dir, ".gitkeep").touch(exist_ok=True)

    if len(os.listdir(cfg.deps_java_dir)) == 1:
        jdk_tmp_true = [x for x in cfg.tmp_dir.iterdir() if x.is_dir() and x.name.startswith("jdk-" + cfg.java_version)]
        if not jdk_tmp_true:
            gui.print_msg("Downloading java jdk...", style=gui.style.info)
            jdk_tmp_dir = Path(cfg.tmp_dir, jdk.install(cfg.java_version, path=cfg.tmp_dir))
        else:
            jdk_tmp_dir = jdk_tmp_true[0]

        gui.print_msg("Optimizing java jdk...", style=gui.style.info)

        jlink = Path(jdk_tmp_dir, "bin", "jlink.exe") if jdk.OS == "windows" else Path(jdk_tmp_dir, "bin", "jlink")

        modules = [
            "java.base",
            "java.datatransfer",
            "java.desktop",
            "java.management",
            "java.net.http",
            "java.security.jgss",
            "java.sql",
            "java.sql.rowset",
            "java.xml",
            "jdk.jartool",
            "jdk.net",
            "jdk.unsupported",
            "jdk.unsupported.desktop",
            "jdk.xml.dom",
            "jdk.zipfs",
        ]

        proc = subprocess.Popen(
            [
                str(jlink),
                "--no-header-files",
                "--no-man-pages",
                "--compress=2",
                "--strip-debug",
                "--add-modules",
                ",".join(modules),
                "--output",
                str(cfg.deps_java_dir),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        result = proc.communicate()[1]
        if "Error:" in result:
            gui.print_msg(result, style=gui.style.warning)
            sys.exit()
        else:
            shutil.rmtree(jdk_tmp_dir)


def _jars(cfg):
    dl = Downloader()
    cfg.deps_jar_dir.mkdir(parents=True, exist_ok=True)
    Path(cfg.deps_jar_dir, ".gitkeep").touch(exist_ok=True)

    jars = [
        "org.xerial:sqlite-jdbc:jar:3.42.0.0",
        "org.postgresql:postgresql:jar:42.6.0",
        "mysql:mysql-connector-java:jar:8.0.30",
        "https://www.sql-workbench.eu/Workbench-Build129.zip::sqlworkbench.jar",
        "com.microsoft.sqlserver:mssql-jdbc:jar:12.2.0.jre11",
        "com.h2database:h2:jar:1.4.199",
        "com.oracle.database.jdbc:ojdbc10:jar:19.19.0.0",
    ]

    for jar in jars:
        if jar.startswith("http"):
            url, file_check = jar.split("::")
            if Path(cfg.deps_jar_dir, file_check).is_file():
                continue

            result = None
            fil = Path(cfg.tmp_dir, url.split("/")[-1])
            if not fil.is_file():
                gui.print_msg("Downloading " + fil.name + " from " + url, style=gui.style.info)

                with requests.get(url, stream=True) as r:
                    with open(fil, "wb") as f:
                        shutil.copyfileobj(r.raw, f)

            if fil.suffix == ".zip":
                result = shutil.unpack_archive(fil, cfg.deps_jar_dir)
                if not result:
                    fil.unlink()
            else:
                result = shutil.move(fil, Path(cfg.deps_jar_dir, url.split("/")[-1]))

            if result:
                gui.print_msg(result, style=gui.style.warning)
                sys.exit()

            continue

        artifact = Artifact.parse(jar)
        jar_file = Path(cfg.deps_jar_dir, artifact.artifact_id + "." + artifact.extension)
        if not jar_file.is_file():
            dl.download(artifact, filename=jar_file)


def _editor(cfg):
    if not len(os.listdir(cfg.deps_editor_dir)) > 1:
        tmp_editor_dir = Path(cfg.tmp_dir, "editor")
        tmp_editor_dir.mkdir(parents=True, exist_ok=True)
        fil = Path(tmp_editor_dir, cfg.editor_url.split("/")[-1])

        if not fil.is_file():
            gui.print_msg("Downloading " + fil.name + " from " + cfg.edit_url, style=gui.style.info)

            with requests.get(cfg.edit_url, stream=True) as r:
                with open(fil, "wb") as f:
                    shutil.copyfileobj(r.raw, f)

        shutil.unpack_archive(fil, tmp_editor_dir)
        sub_dirs = [x for x in tmp_editor_dir.iterdir() if x.is_dir()]
        if sub_dirs:
            shutil.copytree(sub_dirs[0], cfg.deps_editor_dir, dirs_exist_ok=True)
            shutil.rmtree(tmp_editor_dir)

    if not cfg.shfmt_bin.is_file():
        gui.print_msg("Downloading " + cfg.shfmt_bin.name + " from " + cfg.shfmt_url, style=gui.style.info)

        with requests.get(cfg.shfmt_url, stream=True) as r:
            with open(cfg.shfmt_bin, "wb") as f:
                shutil.copyfileobj(r.raw, f)

    if os.name == "posix" and cfg.shfmt_bin.is_file():
        os.chmod(cfg.shfmt_bin, os.stat(cfg.shfmt_bin).st_mode | 0o100)
