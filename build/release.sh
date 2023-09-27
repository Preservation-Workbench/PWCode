#!/bin/bash

SCRIPTPATH="$(dirname "$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || echo "$0")")"

# shellcheck disable=SC1090
source "$SCRIPTPATH"/make.sh

PWCODE_REPO="https://github.com/Preservation-Workbench/PWCode"
PWCODE_DIR="$(dirname "$SCRIPTPATH")"/build/release/pwcode-"$PWCODE_VERSION"-linux64

if [ -d "$PWCODE_DIR" ]; then rm -Rf "$PWCODE_DIR"; fi
git clone "$PWCODE_REPO" "$PWCODE_DIR"

echo "$PWCODE_REPO"

install_rust
install_pyapp
build_pwcode "$PWCODE_DIR"

"$PWCODE_DIR"/pwcode install

# TODO: Fiks så installerer deps utover python deps uten å kjøre fullverdig kommando
# tar -zcvf archive-name.tar.gz source-directory-name

# TODO: SJekk mot resulat av "git status" først for å sjekke at alle endringer er med
