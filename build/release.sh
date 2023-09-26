#!/bin/bash

SCRIPTPATH="$(dirname "$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || echo "$0")")"

# TODO: Git clone her først og så kjøre make mot PWCode i en tmp-mappe? Må det for å sikre at ikke laster opp egne data!

# shellcheck disable=SC1090
source "$SCRIPTPATH"/make.sh
