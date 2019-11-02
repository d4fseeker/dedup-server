
# dedup-server

# Intro
"Dedup Server Suite" is a collection of tools for the processing, managing and restoring of deduplicated block-level backups created by [d4fseeker/dedup-client](https://github.com/d4fseeker/dedup-client) by saving them to a data directory which includes a sqlite3 management file.

For maximum performance, dedup uses xxhash's 64bit hash in hex format to represent and reference blocks. xxhash is non-cryptographic and while accidental collisions are at considered at an acceptably low risk, xxhash is non-cryptographic and may be suspecitble to plaintext collision attacks. This may make datastore poisoning possible, it is hereby strongly recommended to consider not mixing different users or blockdevices  in a single datastore. This will however risk of greatly reducing the storage improvements provided by dedup.

Dedup blocks are split by a predefined blocksize which must be identical for all backups in a single datadir to reduce the risk of hash collision.

# Tools
## depot.py
depot.py processes the tar file produced by dedup-client and imports it into the datastore with a freely chooseable hostname, backupname

# Chaining
## Examples

### Full backup chain started from the backup host to the client through SSH

This assumes that the backup host has SSH access to the root account and that a filesystem snapshot has already been created.

    python3 depot-list-hashes.py --dir ../datadir/ | ssh root@CLIENTHOST "cat - | python3 ./dedup-client/dedup.py --bs 1M --dev /dev/centos_dedup2/root_snap" | python3 depot.py --dir ../datadir/ --host dedup2 --name test4
