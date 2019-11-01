"""
Depot-Clean - Cleans up database and datastore
"""

import argparse,humanfriendly,logging,os,time       #Helpers
from delib import Delib,DelibDataDir    #Dedup-Server

LOGLEVEL=logging.DEBUG
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DepotClean(Delib):

    VERSION = 2019.305 #Year.Yearday

    def __init__(self,dir,fail_after):
        logging.info("Datastore directory {}".format(dir))
        self.data = DelibDataDir(dir)

        #Mark old pending backups as failed
        logging.debug("Marking pending backups older than {}s as failed".format(fail_after))
        older_than = int(time.time()) - fail_after
        self.data.cur.execute("UPDATE backups SET state = 'failed' WHERE state = 'pending' AND time_imported < :olderthan",{ "olderthan": older_than })
        logging.warn("Marked {} pending backups older than {} as failed".format(self.data.cur.rowcount,humanfriendly.format_timespan(fail_after)))

        #Delete backup-block links where backup does not exist or has been removed
        logging.debug("Removing backup-block reference for non-existant, failed, deleted backups")
        self.data.cur.execute("DELETE FROM backup_blocks WHERE NOT EXISTS ( SELECT ROWID FROM backups WHERE ROWID = backup_blocks.backup AND state NOT IN ('failed','deleted'));")
        logging.warn("Deleted {} backup-block references".format(self.data.cur.rowcount))

        #Delete non-referenced blocks in database
        logging.debug("Removing non-referenced block entries if there are no pending backups")
        res=self.data.cur.execute("SELECT COUNT(ROWID) FROM backups WHERE state = 'pending'").fetchone()
        if(res[0] == 0):
            logging.info("Skipping removing non-referenced block entries: {} pending backups".format(res[0]))
        else:
            logging.info("Removing non-referenced block entries; no pending backups ")
            self.data.cur.execute("DELETE FROM blocks WHERE NOT EXISTS ( SELECT hash FROM backup_blocks WHERE block = blocks.hash)")
            logging.warn("Deleted {} block entries".format(self.data.cur.rowcount))

        #Get all remaining blocks from database
        known_files = {}
        self.data.cur.execute("SELECT filename FROM blocks")
        cnt_blocks = 0
        for row in self.data.cur:
            cnt_blocks += 1
            known_files[row["filename"]] = True
        logging.debug("Read {} block entries from database".format(cnt_blocks))

        #Remove blocks on filesystem that are not in DB
        logging.debug("Removing blocks from disk without block entry in database")
        path = self.data.dir + "blocks/"
        cnt_deleted = 0
        for r, d, f in os.walk(path):
            for file in f:
                if file not in known_files:
                    cnt_deleted += 1
                    logging.debug("Found orphaned block: {}".format(file))
                    os.remove(path+file)
        logging.warn("Deleted {} orphaned blocks from datadir".format(cnt_deleted))






def parse_arguments():
     parser = argparse.ArgumentParser()
     parser.add_argument("--dir",nargs=1,required=True,help="Datablock directory")
     parser.add_argument("--fail-after",nargs=1,required=False,default=["1d"],help="Fail pending backups after (Default: 1d)")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    logging.debug("Called: __main__")
    args = parse_arguments()
    logging.info("Starting DepotClean()")
    fail_after = humanfriendly.parse_timespan(args.fail_after[0])
    dedup = DepotClean(dir=args.dir[0],fail_after=fail_after)
