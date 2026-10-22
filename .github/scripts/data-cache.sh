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
    tar -C data -czf - . | "${enc[@]}" -e -out "$blob"
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
