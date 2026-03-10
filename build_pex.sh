#!/usr/bin/env bash
# build_pex.sh — Builds a PEX (Python EXecutable) for the Kafka CLI tool.
#
# Usage:
#   ./build_pex.sh [output_file]
#
# Defaults to "kafka-cli.pex" if no argument is given.
#
# On Unix/Mac the resulting file is marked executable and can be run directly:
#   ./kafka-cli.pex --help
#   ./kafka-cli.pex -b localhost:9092 consume -t my-topic

set -euo pipefail

OUTPUT_FILE="${1:-kafka-cli.pex}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"

# ── Locate venv ────────────────────────────────────────────────────────────
if [[ ! -f "$VENV_DIR/bin/python" ]]; then
    echo "ERROR: venv not found at '$VENV_DIR'." >&2
    echo "       Create it first: python3 -m venv venv && venv/bin/pip install -r requirements.txt" >&2
    exit 1
fi

PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"

# ── Install pex ────────────────────────────────────────────────────────────
echo "==> Installing / upgrading pex into venv..."
"$PIP" install --quiet --upgrade pex

# ── Build PEX ──────────────────────────────────────────────────────────────
echo "==> Building '$OUTPUT_FILE'..."

"$PYTHON" -m pex \
    "kafka-python>=2.0.0" \
    "jsonpath-ng>=1.6.0" \
    "click>=8.0.0" \
    --sources-directory "$SCRIPT_DIR" \
    --entry-point kafka_cli:cli \
    --python-shebang "/usr/bin/env python3" \
    --interpreter-constraint "CPython>=3.13" \
    --output-file "$OUTPUT_FILE"

chmod +x "$OUTPUT_FILE"

SIZE_MB=$(du -sm "$OUTPUT_FILE" | cut -f1)
echo ""
echo "==> Success!  Created: $OUTPUT_FILE  (~${SIZE_MB} MB)"
echo ""
echo "Usage examples:"
echo "  ./$OUTPUT_FILE --help"
echo "  ./$OUTPUT_FILE -b localhost:9092 consume -t my-topic"
echo "  ./$OUTPUT_FILE -b localhost:9092 consume -t my-topic --from-beginning --pretty"
echo "  ./$OUTPUT_FILE -b localhost:9092 produce -t my-topic -m '{\"hello\":\"world\"}'"
echo ""
echo "Environment variables are also supported:"
echo "  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092"
echo "  ./$OUTPUT_FILE consume -t my-topic"
