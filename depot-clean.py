#!/usr/bin/env python3

"""
Depot-Clean - Cleans up database and datastore
"""

import argparse,argparse_logging,humanfriendly,logging,os,time       #Helpers
from delib import Delib,DelibDataDir    #Dedup-Server


class DepotClean(Delib):

    VERSION = 2019.338 #Year.Yearday

    #After how much time (humanfriendly) to mark pending backups as failed
    fail_after = "1d"

    def __init__(self,dir,dry=False):
        self.dry = dry
        if dry:
            logging.warn("Dry-run mode. Will not actually do any changes")
        self.getData(dir)

    def process(self,skip_orphaned=False,skip_unreferenced=False,skip_failafter=False):
        fail_after_seconds = humanfriendly.parse_timespan(self.fail_after)
        fail_older_than = int(time.time()) - fail_after_seconds

        if skip_failafter:
            logging.debug("Clean:failafter skipped. Not failing any pending backups!")
        else:
            logging.debug("Clean:failafter started. Marking pending backups older than %ss as failed",fail_after_seconds)
            #Mark old pending backups as failed
            if self.dry:
                cnt = self.data.cur.execute("SELECT COUNT(rowid) FROM backups WHERE state = 'pending' AND time_imported < :olderthan",{ "olderthan": fail_older_than }).fetchone()[0]
            else:
                self.data.cur.execute("UPDATE backups SET state = 'failed' WHERE state = 'pending' AND time_imported < :olderthan",{ "olderthan": fail_older_than })
                cnt = self.data.cur.rowcount
            logging.warn("Marked %d pending backups older than %s as failed",cnt,self.fail_after)


        if skip_unreferenced:
            logging.debug("Clean:unreferenced skipped. Not removing any unreferenced blocklists")
        else:
            logging.debug("Clean:unreferenced started.")
            #Delete backup-block links where backup does not exist or has been removed
            logging.debug("Removing backup-block reference for non-existant, failed, deleted backups")
            if self.dry:
                cnt = self.data.cur.execute("SELECT COUNT(rowid) FROM backup_blocks WHERE NOT EXISTS ( SELECT ROWID FROM backups WHERE ROWID = backup_blocks.backup AND state NOT IN ('failed','deleted'))").fetchone()[0]
            else:
                self.data.cur.execute("DELETE FROM backup_blocks WHERE NOT EXISTS ( SELECT ROWID FROM backups WHERE ROWID = backup_blocks.backup AND state NOT IN ('failed','deleted'));")
                cnt = self.data.cur.rowcount
            if cnt:
                logging.warn("Deleted %d backup-block references",self.data.cur.rowcount)
            else:
                logging.info("No unreferenced backup-block entries found.")

        #Delete non-referenced blocks in database
        if skip_orphaned:
            logging.debug("Clean:orphaned skipped. Not removing any blocks without corresponding backup")
        else:
            logging.debug("Clean:orphaned started.")


            if self.dry:
                cnt = self.data.cur.execute("SELECT COUNT(rowid) FROM blocks WHERE NOT EXISTS ( SELECT hash FROM backup_blocks WHERE block = blocks.hash ) AND time_imported < :olderthan",{ "olderthan": fail_older_than }).fetchone()[0]
            else:
                self.data.cur.execute("DELETE FROM blocks WHERE NOT EXISTS ( SELECT hash FROM backup_blocks WHERE block = blocks.hash ) AND time_imported < :olderthan",{ "olderthan": fail_older_than })
                cnt = self.data.cur.rowcount
                self.data.db.commit()
            logging.info("Removed %d orphaned block entries that are older than fail_after=%s",cnt,self.fail_after)

            logging.info("Removing blocks without database entry")
            known_files = {} #Use dict for speed
            known_files_cnt = 0
            for row in self.data.cur.execute("SELECT filename FROM blocks"):
                known_files_cnt += 1
                known_files[row["filename"]] = True
            logging.debug("Loaded %d hashes from database",known_files_cnt)

            path = self.data.dir + "blocks/"
            cnt_deleted = 0
            for r, d, f in os.walk(path):
                for file in f:
                    if file not in known_files:
                        cnt_deleted += 1
                        logging.debug("Found orphaned block: {}".format(file))
                        if not self.dry:
                            os.remove(path+file)
            logging.info("Removed %d orphaned blocks from filesystem",cnt_deleted)


def parse_arguments():
     parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,description=
        "Cleans up database inconsistencies caused by failed and removed backus.\nRemoves orphaned blocks from database and filesystem."
     )
     argparse_logging.add_log_level_argument(parser)
     parser.add_argument("-d","--dir",required=True,help="Datablock directory")
     parser.add_argument("-n","--dry",action="store_true",help="Dry-run. Do not move or mark damaged elements.")
     parser.add_argument("-a","--fail-after",required=False,default="1d",help="Fail pending backups and orphaned blocks after X time. Default=1d")
     parser.add_argument("-f","--skip-failafter",action="store_true",help="Skip failing pending backups after X time.")
     parser.add_argument("-u","--skip-unreferenced",action="store_true",help="Skip cleaning up backup->block reference table for removed backups")
     parser.add_argument("-o","--skip-orphaned",action="store_true",help="Skip removing block entries and files without related backup and older than --fail-after")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    LOGLEVEL=logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')

    args = parse_arguments()
    depot_clean = DepotClean(dir=args.dir,dry=args.dry)
    depot_clean.fail_after = args.fail_after
    depot_clean.process(skip_failafter=args.skip_failafter,skip_unreferenced=args.skip_unreferenced,skip_orphaned=args.skip_orphaned)
