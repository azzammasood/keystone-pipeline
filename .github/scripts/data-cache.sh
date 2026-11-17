#!/usr/bin/env bash
# Pack or unpack the data directory for the Actions cache, encrypted (ADR-041). The repository is
# public and workflows of pull requests from forks can restore caches of the base branch, while
# data/ holds salted actor keys (silver, repo_actor_day): the cache only ever holds ciphertext.
# The passphrase is KEYSTONE_ACTOR_SALT (a secret forks never get), stretched with PBKDF2.
# Usage: data-cache.sh pack|unpack   (file: data-cache/data.tar.gz.enc)
set -euo pipefail

: "${KEYSTONE_ACTOR_SALT:?secret KEYSTONE_ACTOR_SALT is not set}"
blob=data-cache/data.tar.gz.enc
enc=(openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -salt -pass env:KEYSTONE_ACTOR_SALT)

case "${1:-}" in
  pack)
    mkdir -p data-cache
    # Left out (ADR-089), all rebuilt or re-fetched when needed:
    # - warehouse: publish rebuilds it from silver and the rollup (no incremental models)
    #   before any step reads it;
    # - raw/osv/zips: parsed into silver/osv; the next fetch is conditional on the parsed
    #   zip's SHA-256 and downloads only a changed zip;
    # - raw/osv/modified: increment copies nothing reads (the cursor is modified_state.json);
    # - raw/gharchive: ingest deletes each hour after parsing; leftovers are fetched again.
    tar -C data --exclude=./warehouse --exclude=./raw/osv/zips --exclude=./raw/osv/modified \
      --exclude=./raw/gharchive -czf - . | "${enc[@]}" -e -out "$blob"
    echo "packed $(stat -c %s "$blob") bytes"
    ;;
  unpack)
    mkdir -p data
    "${enc[@]}" -d -in "$blob" | tar -C data -xzf -
    rm -f "$blob"
    echo "unpacked"
    ;;
  *)
    echo "usage: $0 pack|unpack" >&2
    exit 2
    ;;
esac
