#!/usr/bin/env python3

"""
Depot-Health - Reports the current state of failed & broken backups + damaged blocks
"""

import argparse,argparse_logging,humanfriendly,logging,os,time,sys       #Helpers
from delib import Delib,DelibBlock,DelibBackup    #Dedup-Server

class DepotHealth(Delib):

    VERSION = 2019.337 #Year.Yearday

    def __init__(self,dir):
        self.getData(dir)

    #Process checking
    def process(self,skip_check_blocks=False,skip_check_backups=False):
        is_ok = True
        if not skip_check_blocks:
            if not self.verifyBlocks():
                is_ok = False
        if not skip_check_backups:
            if not self.verifyBackups():
                is_ok = False

        if is_ok:
            logging.info("Overall State: damaged")
            return False
        else:
            logging.info("Overall State: healthy")
            return True
        return is_ok


    #Verify bad blocks and return list of bad blocks
    def verifyBlocks(self):
        blocks = self.data.BBgetHashes()
        cnt = len(blocks)
        if cnt:
            logging.warn("Have %d damaged blocks",cnt)
            for block in blocks:
                logging.info("-> Hash %s",block)
        else:
            logging.info("Have zero damaged blocks")

    #Verify broken backups by running corresponding checks
    def verifyBackups(self):
        failed_backups = len(self.data.getBackupsByState(DelibBackup.STATE_FAILED))
        if failed_backups:
            logging.warn("Have %d failed backups",failed_backups)
            for failed_backup in failed_backups:
                logging.info("-> Backup %s:%s",failed_backup["host"],failed_backup["name"])
        else:
            logging.info("Have zero failed backups")
        broken_backups = self.data.getBackupsByState(DelibBackup.STATE_BROKEN)
        if broken_backups:
            logging.warn("Have %d failed backups",broken_backups)
            for broken_backup in broken_backups:
                logging.info("-> Backup %s:%s",broken_backup["host"],broken_backup["name"])
        else:
            logging.info("Have zero broken backups")

        if failed_backups or broken_backups:
            return False
        else:
            return True


def parse_arguments():
     parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,description=
        "Reports current datastore health by reporting:\n** backups marked as failed / broken\n** damaged blocks\nRC=1 if anything is broken, otherwise RC=0"
    )
     argparse_logging.add_log_level_argument(parser)
     parser.add_argument("-d","--dir",required=True,help="Datablock directory")
     parser.add_argument("-b","--skip-blocks",action="store_true",help="Skip individual block checking")
     parser.add_argument("-a","--skip-backups",action="store_true",help="Skip backup checking")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    LOGLEVEL=logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')

    args = parse_arguments()
    depot_health = DepotHealth(dir=args.dir)
    check = depot_health.process(skip_check_blocks=args.skip_blocks,skip_check_backups=args.skip_backups)
    if not check:
        sys.exit(1)