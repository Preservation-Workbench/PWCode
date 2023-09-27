#!/bin/bash

SCRIPTPATH="$(dirname "$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || echo "$0")")"

# shellcheck disable=SC1090
source "$SCRIPTPATH"/make.sh

PWCODE_REPO="https://github.com/Preservation-Workbench/PWCode"
PWCODE_BASE_DIR="$(dirname "$SCRIPTPATH")"/build/release/pwcode
PWCODE_DIR="$PWCODE_BASE_DIR"-"$PWCODE_VERSION"-linux64

if ! [ -x "$(command -v git)" ]; then
	cecho "$RED" "git command missing. Exiting script.."
	exit 1
fi

GITCHECK="$(git status --porcelain)"
if [ -n "$GITCHECK" ]; then
	cecho "$RED" "Working directory has UNCOMMITTED CHANGES. Exiting script.."
	exit 1
fi

if [ -d "$PWCODE_BASE_DIR" ]; then rm -Rf "$PWCODE_BASE_DIR"; fi
if [ -d "$PWCODE_DIR" ]; then rm -Rf "$PWCODE_DIR"; fi
git clone "$PWCODE_REPO" "$PWCODE_DIR"

install_rust
install_pyapp
build_pwcode "$PWCODE_DIR"

cd "$PWCODE_DIR" && ./pwcode install
mv "$PWCODE_DIR" "$PWCODE_BASE_DIR"
tar -zcf "$SCRIPTPATH"/pwcode-"$PWCODE_VERSION"-linux64.tar.gz "$PWCODE_BASE_DIR"
