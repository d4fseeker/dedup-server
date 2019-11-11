
# dedup-server

# Intro
## Preface
"Dedup Server Suite" is a collection of tools for the processing, managing and restoring of deduplicated block-level backups created by [d4fseeker/dedup-client](https://github.com/d4fseeker/dedup-client) by saving them to a data directory which includes a sqlite3 management file.

For maximum performance, dedup uses xxhash's 64bit hash in hex format to represent and reference blocks. xxhash is non-cryptographic and while accidental collisions are at considered at an acceptably low risk, xxhash is non-cryptographic and may be suspecitble to plaintext collision attacks. This may make datastore poisoning possible, it is hereby strongly recommended to consider not mixing different users or blockdevices  in a single datastore. This will however risk of greatly reducing the storage improvements provided by dedup.

Dedup blocks are split by a predefined blocksize which must be identical for all backups in a single datadir to reduce the risk of hash collision.

## Installation
Dependencies in Centos/Redhat 7/8  (Server and client)
> yum install python3 python3-pip
> pip3 install tqdm xxhash lz4 humanfriendly

Dependencies in Debian:
#TODO

## Human-friendly
Dedup uses human-friendly time and size formats as provided by the python "humanfriendly" library. For better geometry alignment to storage systems, size formats are understood and outputted in binary format even if not explicitely provided, meaning the format "1M" corresponds to "1MiB", aka 1024 * 1024 bytes.
Dates are always provided in the format YYYY-MM-DD and date-times in the format YYYY-MM-DD_HH-II-SS.

## Machine-friendly
Output that may need to be machine-understood such as dedup-list-backups.py can be switched to json format with a CLI parameter. All logging output is provided on STDERR while STDOUT is reserved for actual data output.

# Tools
## depot.py
depot.py processes the tar file produced by dedup-client and imports it into the datastore with a freely chooseable hostname and backupname. Some additional caracteristics are imported from the backup file (see "File format" further below) for reference. Depot waits for a Dedup-Tar on STDIN and has no STDOUT.
> cat dedup.tar | python3 depot.py --dir /path/to/datadir --host ANY_NAME --name ANY_BACKUP_NAME

## depot-create.py
Creates a new depot. The folder must exist but have no files inside. The blocksize can be provided in human-friendly format (See Intro). depot-create has no STDIN and no STDOUT.
> python3 depot-create.py --dir /path/to/datadir --bs 1M

## depot-clean.py
To remove a backup, it is enough to mark it as "deleted" in the database and let depot-clean remove it on the next run.
depot-clean.py marks backups that crashed/aborted during import as failed (default: after 1 day), removes backup block references of backups that are marked failed or deleted and deletes blocks that are not referenced in the database.
It can be run regularly as cron, chained to a depot.py command or whenever needed depending on the user choices.
> python3 depot-clean.py --dir /path/to/datadir [--fail-after 1d]

## depot-list-backups.py
Returns a list of backups in depot. STDOUT is a human-friendly CLI display by default but can also return CSV or JSON. Backup filters are combinable.

Show all backups:
> python3 depot-list-backups.py --dir /path/to/datadir [--format CLI]

Show backups of a single state:
> python3 depot-list-backups.py --dir /path/to/datadir --state pending [--format CLI]

Show backups of a single host:
> python3 depot-list-backups.py --dir /path/to/datadir --host example.com [--format CLI]

## depot-list-hashes.py
Returns a newline-separated list of all hashes in datadir on STDOUT. Used by dedup.py on STDIN.
> python3 depot-list-hashes.py --dir /path/to/datadir

## depot-verify.py
Checks for each block in the database if it is on disk, decompresses it and verifies if the hash is correct.
Any failed hashes are reported and all backups using failed hashes are marked as "broken".
> python3 depot-verify.py --dir /path/to/datadir

## dedup-restore.py
Streams the original file/blockdevice contents on STDOUT.
> python3 dedup-restore.py --dir /path/to/datadir --host example.com --name backup_name

# Chaining
## Examples

### Full backup chain started from the backup host to the client through SSH

This assumes that the backup host has SSH access to the root account and that a filesystem snapshot has already been created.

> python3 depot-list-hashes.py --dir ../datadir/ | ssh root@CLIENTHOST "cat - | python3 ./dedup-client/dedup.py --bs 1M --dev /dev/centos_dedup2/root_snap" | python3 depot.py --dir ../datadir/ --host dedup2 --name test4

# Use case scenarios
## Deduplicated backups
With the advent of virtual machines and cloud-computing, users expect the server administrator to provide comprehensive reliable backup solutions at low costs and without having to install software agents inside the customer's machine. Virtual machines usually rely on blockdevices, often LVM, for their data storage. Creating copies of a blockdevice is resource-prohibitive and requires significant commitments into bandwith and datastorage capacities. Dedup can greatly increase the backup speed while keeping backup data storage requirements, bandwith requirements and performance cost on the host machine at a minimum.
## (Future) Efficient offline data synchronization
It can prove challenging to update large data storages over the internet or using the internet may not be a practical solution. In this case dedup.py's output tar can be used to migrate file deltas of databases or similar large files. ! Currently dedup does not provide a sync mechanism yet, only the ability to create the deltas with dedup.py !!

# Formats
## Dedup-Tar
Dedup's reliance on UNIX piping/streaming mechanisms led to the usage of the well-known, widely supported and simple file packaging format TAR. The tar file is outputted by dedup.py and imported by depot.py

Dedup-Tar requires a strict order of three "blocks" of files commonly called "header", "body" and "footer". The order of the files inside the header, body and footer respectively is irrelevant. If blocks are interchanged, the stream nature of dedup is no longer given, resulting in depot.py to consider the file as broken and abandon the import.

### Dedup-Tar :: Header
The header contains the following files:
- /backup/host  - The FQDN hostname of the client that dedup.py was running on (for reference only)
- /backup/device - The path of the file or device that was backed up. Imported in depot
- /backup/blocksize - The blocksize of the backup in bytes. Used to verify compatibility with depot's datastore
- /backup/filesize - The size of the file or device in bytes. Imported in depot
- /backup/created - Epoch timestamp of the time the dedup was started. Imported in depot
- /dedup/version - The client version of dedup to check compatibility (future use)

### Dedup-Tar :: Body
The body contains all new blocks compressed with lz4 and named with their corresponding xxhash64 hash.
- /newblocks/{XXHASH64}.lz4

### Dedup-Tar :: Footer
The footer contains the following files:,
- /backup/list - A sequential newline-separated list of all blocks in the backup referenced by their hash.

# Datadir
The datadir has by default a file and a folder within:
- $datadir/blocks - A folder for all blocks in the datadir as separate files with {HASH}.lz4 as filename
- $datadir/db.sqlite3 - The management database in SQLite3 file format.

## db.sqlite3
Tables in the database:
- settings - All datadir settings. Currently only contains the blocksize
- blocks - All blocks with their original size, compressed size, filename (inside blocks/ folder ), time of first import, and compression info
- backups - All backups with their name, host, backupid (=ROWID) and additional information
- backup_blocks - Linking backups to backup_blocks with the additional information of position.+


# TODO
## A note about the current state
Dedup-Suite is currently a work in progress. It bases on more than 5 years of experience from similar tools but will continue to grow in function and performance. The software is currently being developed from passion and interest in deduplication technologies without immediate usage by the author. If you use it, please let me know!

Some features may not be fully tested yet. While the author feels confident that dedup can be used as-is today, every user is encouraged to run their own tests and provide feedback or contribute to the software.

## What's to come
Especially self-validation, self-checking and as far as possible self-healing are big "TODO" points for the database.
Additionally, the code needs some more cleanup, commenting and error checking to make sure it works reliably in automated scenarios.
