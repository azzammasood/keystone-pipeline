#!/usr/bin/env bash
# Encrypt or decrypt the internal per-actor table `repo_actor_month` (salted actor keys; ADR-045).
# The backfill uploads it to the window's release only as ciphertext
# (`repo_actor_month-<chunk>.parquet.enc`); the backtest decrypts it on the runner into a Parquet
# directory that `keystone score backtest --rollup DIR` reads, and never uploads the plaintext.
# Cipher: AES-256-CBC, PBKDF2 (600,000 iterations), random salt; the same openssl form as
# data-cache.sh, with its own passphrase KEYSTONE_ACTOR_DATA_KEY (owner, 2026-10-22).
# Usage: actor-data.sh encrypt IN.parquet OUT.parquet.enc
#        actor-data.sh decrypt SRC_DIR DEST_DIR   (every *.parquet.enc in SRC_DIR)
set -euo pipefail

: "${KEYSTONE_ACTOR_DATA_KEY:?secret KEYSTONE_ACTOR_DATA_KEY is not set}"
enc=(openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -salt -pass env:KEYSTONE_ACTOR_DATA_KEY)

case "${1:-}" in
  encrypt)
    "${enc[@]}" -e -in "$2" -out "$3"
    ;;
  decrypt)
    mkdir -p "$3"
    n=0
    for f in "$2"/*.parquet.enc; do
      [ -e "$f" ] || continue
      out="$3/$(basename "${f%.enc}")"
      "${enc[@]}" -d -in "$f" -out "$out"
      n=$((n + 1))
    done
    echo "decrypted $n file(s) into $3"
    ;;
  *)
    echo "usage: $0 encrypt IN OUT | decrypt SRC_DIR DEST_DIR" >&2
    exit 2
    ;;
esac
