#!/bin/bash
SCRIPTPATH="$(dirname "$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || echo "$0")")"
RED="\033[1;31m"
GREEN="\033[1;32m" # <-- [0 is not bold
CYAN="\033[1;36m"

download() {
	if [ -f "$1" ]; then return 1; fi
	if [ ! -f "$2" ]; then wget "$3" -O "$2"; fi
}

cecho() {
	NC="\033[0m" # No Color
	# shellcheck disable=SC2059
	printf "${1}${2} ${NC}\n"
}

recho() {
	# shellcheck disable=SC2181
	if [[ $? -eq 0 ]]; then
		cecho "$GREEN" "Done!"
	else
		cecho "$RED" "Operation failed. Exiting script.."
		exit 1
	fi
}

if ! [ -x "$(command -v rustc)" ]; then
	cecho "$CYAN" "Installing rust..."
	cd /tmp && curl --proto '=https' --tlsv1.3 https://sh.rustup.rs -sSf | sh
	recho $?
fi

PYAPP_DIR=/tmp/pyapp
if [ ! -f $PYAPP_DIR/build.rs ]; then
	cecho "$CYAN" "Downloading pyapp source code..."
	PYAPP_SRC=/tmp/pyapp.tar.gz
	URL=https://github.com/ofek/pyapp/releases/download/v0.10.1/source.tar.gz
	download $PYAPP_SRC $PYAPP_SRC $URL
	recho $?

	cecho "$CYAN" "Unpacking pyapp source code..."
	mkdir -p $PYAPP_DIR
	tar -xf $PYAPP_SRC -C $PYAPP_DIR --strip-components=1
	recho $?
fi

PWCODE_DIR="$(dirname "$SCRIPTPATH")"
cecho "$CYAN" "Running cargo build..."
cd $PYAPP_DIR && PYAPP_PROJECT_VERSION=0.1 PYAPP_PROJECT_NAME=pwcode cargo build --release
recho $?

cecho "$CYAN" "Running cargo install..."
PYAPP_PROJECT_VERSION=0.1 PYAPP_PROJECT_NAME=pwcode PYAPP_EXEC_SCRIPT="$SCRIPTPATH"/install_run.py \
	PYAPP_PYTHON_VERSION=3.11 PYAPP_DISTRIBUTION_EMBED=1 PYAPP_FULL_ISOLATION=1 PYAPP_SKIP_INSTALL=1 \
	cargo install pyapp --force --root "$PWCODE_DIR"
recho $?

cecho "$CYAN" "cleanup..."
mv -f "$PWCODE_DIR"/bin/pyapp "$PWCODE_DIR"/pwcode && rm -rdf "${PWCODE_DIR:?}"/bin/ && rm "$PWCODE_DIR"/.crates*
recho $?

cecho "$GREEN" "Binary $PWCODE_DIR/pwcode built successfully."
