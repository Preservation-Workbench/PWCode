#!/bin/bash

SCRIPTPATH="$(dirname "$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || echo "$0")")"

# shellcheck disable=SC1090
source "$SCRIPTPATH"/make.sh

PWCODE_REPO="https://github.com/Preservation-Workbench/PWCode"
PWCODE_DIR="$(dirname "$SCRIPTPATH")"/build/release/pwcode-"$PWCODE_VERSION"-linux64

GITCHECK="$(git status --porcelain)"
if [ -n "$GITCHECK" ]; then
	cecho "$RED" "Working directory has UNCOMMITTED CHANGES. Exiting script.."
	exit 1
fi

exit

if [ -d "$PWCODE_DIR" ]; then rm -Rf "$PWCODE_DIR"; fi
git clone "$PWCODE_REPO" "$PWCODE_DIR"

echo "$PWCODE_DIR"/pwcode

install_rust
install_pyapp
build_pwcode "$PWCODE_DIR"

cd "$PWCODE_DIR" && ./pwcode install
tar -zcvf archive-name.tar.gz source-directory-name
