"""
Depot-Verify - Datastore hash <-> block validation
"""

import argparse,humanfriendly,logging,os       #Helpers
from delib import Delib,DelibDataDir,DelibBlock    #Dedup-Server

LOGLEVEL=logging.INFO
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DepotVerify(Delib):

    VERSION = 2019.300 #Year.Yearday

    def __init__(self,dir):
        logging.info("Datastore directory {}".format(dir))
        self.data = DelibDataDir(dir)
        bad_blocks = []
        bad_backups = {}

        logging.info("Verifying blocks")
        for row in self.data.cur.execute("SELECT * FROM blocks ORDER BY ROWID ASC"):
            logging.debug("Verifying {}".format(row["hash"]))
            filepath=self.data.dir + "/blocks/" + row["filename"]
            try:
                block = DelibBlock.fromFile (file=filepath , compressed = row["compressed"] )
                if block.getHash() != row["hash"]:
                    logging.error("{} should have hash {} but has {}".format(filepath,row["hash"],block.getHash()))
                    bad_blocks.append(row["hash"])
            except Exception as e:
                logging.error("Could not read block {}, {}".format(row["hash"],str(e)))
                bad_blocks.append(row["hash"])

        if len(bad_blocks) == 0:
            logging.info("Success! No failed blocks!")
            return
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






def parse_arguments():
     parser = argparse.ArgumentParser()
     parser.add_argument("--dir",nargs=1,required=True,help="Datablock directory")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    logging.debug("Called: __main__")
    args = parse_arguments()
    logging.info("Starting DepotVerify()")
    dedup = DepotVerify(dir=args.dir[0])
