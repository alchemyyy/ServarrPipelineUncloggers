#!/usr/bin/env bash

set -euo pipefail

script_directory="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_directory"

scripts=(
    "ServarrIndexerForceTester.py"
    "ServarrForceImporter.py"
    "SonarrMissingEpisodeSearcher.py"
    "SonarrTBAEpisodeRefresher.py"
)

for script in "${scripts[@]}"; do
    if [[ ! -f "$script" ]]; then
        printf 'Missing script: %s\n' "$script" >&2
        exit 1
    fi

    printf 'Building %s...\n' "$script"
    pyinstaller --onefile "$script"
done

printf 'Executables written to %s/dist\n' "$script_directory"
