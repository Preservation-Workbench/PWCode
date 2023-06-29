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

# Python Library Imports:
import os
from pathlib import Path
import jpype as jp


def init_jvm(java_home, jars_dir):
    jvm_path = Path(java_home, "lib", "server", "libjvm.so")
    if os.name == "nt":
        Path(java_home, "lib", "server", "jvm.dll")

    if jp.isJVMStarted():
        return

    jar_files = []
    for f in os.listdir(jars_dir):
        if f.endswith('.jar'):
            jar_files.append(str(Path(jars_dir,f)))

    jp.startJVM(str(jvm_path), "-Djava.class.path={}".format(':'.join(jar_files)))
