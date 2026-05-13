#!/usr/bin/env bash
# Generate a test JWT signed with the dev RSA private key for local manual testing.
# Usage: ./scripts/generate-dev-jwt.sh [subject] [expiry-seconds]
# Requires: openssl (available on Linux, macOS, and Windows via Git Bash or WSL)
#
# The private key is read from DEV_PRIVATE_KEY_PATH (docker-compose.env) or defaults
# to dev-private.pem in the repo root. Generate it once with:
#   openssl genrsa -out dev-private.pem 2048
#   openssl rsa -in dev-private.pem -pubout -out platform-public.pem

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load docker-compose.env if present (for DEV_PRIVATE_KEY_PATH override)
ENV_FILE="$REPO_ROOT/docker-compose.env"
if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a; source "$ENV_FILE"; set +a
fi

PRIVATE_KEY="${DEV_PRIVATE_KEY_PATH:-$REPO_ROOT/dev-private.pem}"

if [[ ! -f "$PRIVATE_KEY" ]]; then
    echo "ERROR: private key not found at $PRIVATE_KEY" >&2
    echo "Generate it with:" >&2
    echo "  openssl genrsa -out dev-private.pem 2048" >&2
    echo "  openssl rsa -in dev-private.pem -pubout -out platform-public.pem" >&2
    exit 1
fi

SUBJECT="${1:-dev-user}"
EXPIRY_SECS="${2:-3600}"
NOW=$(date +%s)
EXP=$((NOW + EXPIRY_SECS))

HEADER='{"alg":"RS256","typ":"JWT"}'
PAYLOAD="{\"sub\":\"${SUBJECT}\",\"iat\":${NOW},\"exp\":${EXP},\"iss\":\"event-viewer-dev\"}"

b64url() {
    openssl base64 -A | tr '+/' '-_' | tr -d '='
}

HEADER_B64=$(printf '%s' "$HEADER" | b64url)
PAYLOAD_B64=$(printf '%s' "$PAYLOAD" | b64url)
SIGNING_INPUT="${HEADER_B64}.${PAYLOAD_B64}"

TMP_SIG=$(mktemp)
printf '%s' "$SIGNING_INPUT" | openssl dgst -sha256 -sign "$PRIVATE_KEY" -binary > "$TMP_SIG"
SIGNATURE=$(openssl base64 -A -in "$TMP_SIG" | tr '+/' '-_' | tr -d '=')
rm -f "$TMP_SIG"

echo "${SIGNING_INPUT}.${SIGNATURE}"
