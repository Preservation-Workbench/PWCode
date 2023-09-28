#!/bin/bash

SCRIPTPATH="$(dirname "$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || echo "$0")")"
RELEASE_DIR="$(dirname "$SCRIPTPATH")"/build/release/
PWCODE_DIR=$RELEASE_DIR/PWCode

# shellcheck disable=SC1090
source "$SCRIPTPATH"/make.sh

if ! [ -x "$(command -v git)" ]; then
	cecho "$RED" "git command missing. Exiting script.."
	exit 1
fi

GITCHECK="$(git status --porcelain)"
if [ -n "$GITCHECK" ]; then
	cecho "$RED" "Working directory has UNCOMMITTED CHANGES. Exiting script.."
	exit 1
fi

if [ -d "$PWCODE_DIR" ]; then rm -Rf "$PWCODE_DIR"; fi
git clone "https://github.com/Preservation-Workbench/PWCode" "$PWCODE_DIR"

install_rust
install_pyapp
build_pwcode "$PWCODE_DIR"

cd "$PWCODE_DIR" && ./pwcode install
cd RELEASE_DIR && tar -zcf pwcode-"$PWCODE_VERSION"-linux64.tar.gz "$PWCODE_DIR"
rm -Rf "$PWCODE_DIR"
