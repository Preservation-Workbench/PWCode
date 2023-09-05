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

import re

from dulwich.repo import Repo
from dulwich import porcelain
from dulwich.porcelain import open_repo_closing
import gui


def get_latest_hash(repo):
    with open_repo_closing(repo) as r:
        return r.head().decode("ascii")


def run(main_cfg):
    """
    Run from cli like this:
    On Linux: ./pwcode script --path scripts/git_client.py
    """

    # https://www.dulwich.io/docs/api/dulwich.porcelain.html
    # https://github.com/slaveofcode/gitty

    repo = Repo('.')
    (_, ref), _ = repo.refs.follow(b'HEAD')
    match = re.search(r'/([^/]+)$', ref.decode('utf-8'))
    gui.print_msg(match[1], style=gui.style.info)

    config = Repo('.').get_config()
    gui.print_msg(config.get(('remote', 'origin'), 'url').decode("utf-8"), style=gui.style.info)

    gui.print_msg(get_latest_hash(repo), style=gui.style.info)

    status = porcelain.status(repo)
    # gui.print_msg(status, style=gui.style.info)
    gui.print_msg(status.untracked, style=gui.style.info)
    gui.print_msg(status.unstaged, style=gui.style.info)

    # open("myrepo/testfile", "w").write("data")
    # porcelain.add(repo, "myrepo/testfile")
    # porcelain.commit(repo, "A sample commit")
