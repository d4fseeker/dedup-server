"""
Depot-Verify - Datastore hash <-> block validation
"""

import argparse,humanfriendly,logging,os       #Helpers
from delib import Delib,DelibDataDir,DelibBlock    #Dedup-Server

LOGLEVEL=logging.INFO
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DepotVerify(Delib):

    VERSION = 2019.300 #Year.Yearday

    def __init__(self,dir,dry=False):
        self.dry = dry
        self.getData(dir)

    #Process checking
    def process(self,skip_check_blocks=False,skip_check_backups=False):):
        if not skip_check_blocks:
            bad_blocks = self.verifyBlocks()
            if not self.dry and len(bad_blocks):
                self.moveBadBlocks(bad_blocks)


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
                    bad_blocks.push(hash
            except Exception as e:
                logging.error("Block %s could not be loaded. Missing or corrupted")
                bad_blocks.push(hash)
        logging.info("Done verifying blocks.")
        return bad_blocks






        if len(bad_blocks) == 0:
            logging.info("Success! No failed blocks!")
        else:
            for bad_block in bad_blocks:
                for bad_backup in self.data.cur.execute("SELECT ba.rowid,ba.name,ba.host,ba.state FROM blocks bl LEFT JOIN backup_blocks bb ON bl.hash = bb.block LEFT JOIN backups ba ON bb.backup = ba.rowid WHERE bl.hash = :hash AND ba.state = 'ready' OR state = 'broken'",{ "hash": bad_block }):
                    if bad_backup["state"] == "ready":
                        logging.warn("Marking backup {} (host {}, name {}) as broken due to at least hash {} failing.".format(bad_backup["rowid"],bad_backup["host"],bad_backup["name"],bad_block))
                        self.data.cur.execute("UPDATE backups SET state = 'failed' WHERE rowid = :rowid", {'rowid': bad_backup["rowid"] } )
                        self.data.db.commit()
                    bad_backups[bad_backup["host"]+":"+bad_backup["name"]] = True

            all_failed_backups = ", ".join(list(bad_backups.keys()))
            all_failed_hashes = ", ".join(bad_blocks)
            logging.error("All failed xhashes: "+all_failed_hashes)
            logging.error("All failed backups: "+all_failed_backups)

            logging.warn("Failed with bad blocks!")










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
