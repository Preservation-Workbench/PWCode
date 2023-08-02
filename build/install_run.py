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
import runpy
import site
import shutil
import subprocess
from pathlib import Path

# BASE PATHS:
base_dir = Path(os.environ["PYAPP"]).parent.absolute()
tmp_dir = Path(base_dir, "projects", "tmp")
tmp_dir.mkdir(parents=True, exist_ok=True)
Path(tmp_dir, ".gitkeep").touch(exist_ok=True)

# PYTHON LIBS:
src_dir = Path(base_dir, "src")
site.addsitedir(src_dir)
deps_python_dir = Path(base_dir, "deps", "python")
site.addsitedir(deps_python_dir)
deps_python_dir.mkdir(parents=True, exist_ok=True)
Path(deps_python_dir, ".gitkeep").touch(exist_ok=True)
if len(os.listdir(deps_python_dir)) == 1:
    print("Installing python dependencies...")
    req_file = Path(base_dir, "requirements.txt")
    cmd = [sys.executable, "-m", "pip", "install", "--target", deps_python_dir, "-r", req_file]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, universal_newlines=True)
    result = proc.communicate()[1]
    if "ERROR:" in result:
        print(result)
        sys.exit()

# JDK:
import jdk

version = "11"
deps_java_dir = Path(base_dir, "deps", "java." + jdk.OS)
if not deps_java_dir.is_dir():
    jdk_tmp_true = [x for x in tmp_dir.iterdir() if x.is_dir() and x.name.startswith("jdk-" + version)]
    if not jdk_tmp_true:
        print("Downloading java jdk...")
        jdk_tmp_dir = Path(tmp_dir, jdk.install(version, path=tmp_dir))
    else:
        jdk_tmp_dir = jdk_tmp_true[0]

    print("Optimizing java jdk...")

    min = a if a < b else b
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
            str(deps_java_dir),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    result = proc.communicate()[1]
    if "Error:" in result:
        print(result)
        sys.exit()
    else:
        shutil.rmtree(jdk_tmp_dir)

# JARS:
from maven_artifact import Downloader
from maven_artifact.artifact import Artifact
import requests

dl = Downloader()
deps_jar_dir = Path(base_dir, "deps", "jars")
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
        if Path(deps_jar_dir, file_check).is_file():
            continue

        result = None
        fil = Path(tmp_dir, url.split("/")[-1])
        if not fil.is_file():
            print("Downloading " + fil.name + " from " + url)
            with requests.get(url, stream=True) as r:
                with open(fil, "wb") as f:
                    shutil.copyfileobj(r.raw, f)

        if fil.suffix == ".zip":
            result = shutil.unpack_archive(fil, deps_jar_dir)
            if not result:
                fil.unlink()
        else:
            result = shutil.move(fil, Path(deps_jar_dir, url.split("/")[-1]))

        if result:
            print(result)
            sys.exit()

        continue

    artifact = Artifact.parse(jar)
    jar_file = Path(deps_jar_dir, artifact.artifact_id + "." + artifact.extension)
    if not jar_file.is_file():
        dl.download(artifact, filename=jar_file)

# EDITOR:
urls = {
    "linux": "https://github.com/zyedidia/micro/releases/download/v2.0.11/micro-2.0.11-linux64.tar.gz",
    "windows": "https://github.com/zyedidia/micro/releases/download/v2.0.11/micro-2.0.11-win64.zip",
}

url = urls[str(jdk.OS)]
deps_editor_dir = Path(base_dir, "deps", "editor")
if not (deps_editor_dir.is_dir() and len(os.listdir(deps_editor_dir)) > 0):
    tmp_editor_dir = Path(tmp_dir, "editor")
    tmp_editor_dir.mkdir(parents=True, exist_ok=True)
    fil = Path(tmp_editor_dir, url.split("/")[-1])

    if not fil.is_file():
        print("Downloading " + fil.name + " from " + url)
        with requests.get(url, stream=True) as r:
            with open(fil, "wb") as f:
                shutil.copyfileobj(r.raw, f)

    shutil.unpack_archive(fil, tmp_editor_dir)
    sub_dirs = [x for x in tmp_editor_dir.iterdir() if x.is_dir()]
    if sub_dirs:
        shutil.copytree(sub_dirs[0], deps_editor_dir, dirs_exist_ok=True)
        shutil.rmtree(tmp_editor_dir)

# RUN:
runpy.run_path(Path(src_dir, "main.py"), run_name="__main__")
