#!/usr/bin/env python3

"""
Depot-Verify - Datastore hash <-> block validation
"""

import argparse,humanfriendly,logging,os,shutil       #Helpers
from delib import Delib,DelibBlock,DelibBackup    #Dedup-Server

LOGLEVEL=logging.DEBUG
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DepotVerify(Delib):

    VERSION = 2019.300 #Year.Yearday

    def __init__(self,dir,dry=False):
        self.dry = dry
        self.getData(dir)

    #Process checking
    def process(self,skip_check_blocks=False,skip_check_backups=False):
        logging.info("Starting process()")
        if not skip_check_blocks:
            bad_blocks = self.verifyBlocks()
            if not self.dry and len(bad_blocks):
                self._moveBrokenBlocks(bad_blocks)
        if not skip_check_backups:
            bad_backups = self.verifyBackups()
            if not self.dry and len(bad_backups):
                self._markBrokenBackups(bad_backups)
        logging.info("Finished process()")


    #Verify bad blocks and return list of bad blocks
    def verifyBlocks(self):
        bad_blocks = []
        logging.info("Verifying blocks")
        for hash in self.data.getHashes():
            logging.debug("Verifying %s",hash)
            try:
                block = DelibBlock.fromHash(self.data,hash)
                if hash != block.getHash(update=True):
                    logging.error("Block %s failed integrity check. Incorrect hash: %s",hash,block.getHash())
                    bad_blocks.push(hash)
            except Exception as e:
                logging.error("Block %s could not be loaded. %s",hash,str(e))
                bad_blocks.append(hash)
        logging.info("Done verifying blocks.")
        return bad_blocks

    #Move broken blocks from /blocks to /damaged directory
    def _moveBrokenBlocks(self,hashes):
        logging.warn("Moving %d broken blocks",len(hashes))
        dst_path = self.data.dir+"/damaged/"
        orig_path = self.data.dir+"/blocks/"
        for hash in hashes:
            logging.info("Processing %s",hash)
            filename = self.data.DBGetFilename(hash)
            orig_file = orig_path + filename
            dst_file = dst_path + filename + str(int(time.time())) + ".broken"
            #Remove DB entry
            logging.info("Removing blocks entry in DB")
            self.data.db.execute("DELETE FROM blocks WHERE hash = :hash",{"hash": hash})
            logging.info("Moving %s to %s",orig_file,dst_file)
            #Move file and finalize with commit
            shutil.move(orig_file,dst_file)
            self.data.db.commit() #Commit immediately to minimize inconsistenices
        logging.info("Done moving broken blocks")


    #Verify broken backups by running corresponding checks
    def verifyBackups(self):
        logging.info("Verifying backup integrity")
        bad_backups = []
        #Verify all backups that are state "ready"
        for backup_ref in self.data.getBackupsByState("ready"):
            logging.debug("Verifying %s:%s",backup_ref["host"],backup_ref["name"])
            #Load and verify backup
            backup = DelibBackup.fromName(data=self.data,host=backup_ref["host"],name=backup_ref["name"])
            if not backup.verify_continuity(throw_exception=False):
                logging.error("Backup %s:%s failed integrity check.",backup_ref["host"],backup_ref["name"])
                bad_backups.append(backup_ref)
        return bad_backups

    def _markBrokenBackups(self,backup_refs):
        logging.warn("Marking %d broken backups as failed.",len(backup_refs))
        for backup_ref in backup_refs:
            logging.info("Marking %s:%s as failed",backup_ref["host"],backup_ref["name"])
            self.data.cur.execute("UPDATE backups SET state = 'failed' WHERE host = :host AND name = :name", {'host': backup_ref["host"], 'name': backup_ref["name"] } )
        logging.info("Done marking broken backups")



def parse_arguments():
     parser = argparse.ArgumentParser()
     parser.add_argument("--dir",required=True,help="Datablock directory")
     parser.add_argument("--dry",action="store_true",help="Dry-run. Do not move or mark damaged elements.")
     parser.add_argument("--skip-blocks",action="store_true",help="Skip individual block checking")
     parser.add_argument("--skip-backups",action="store_true",help="Skip backup->block completion check")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    logging.debug("Called: __main__")
    args = parse_arguments()
    logging.info("Starting DepotVerify()")
    depot_verify = DepotVerify(dir=args.dir,dry=args.dry)
    depot_verify.process(skip_check_blocks=args.skip_blocks,skip_check_backups=args.skip_backups)
