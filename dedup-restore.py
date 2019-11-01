"""
Depot-Create - Datastore hash list
"""

import argparse,humanfriendly,logging,os       #Helpers
from delib import Delib,DelibDataDir,DelibRestore    #Dedup-Server
from tqdm import tqdm #Progress bar

LOGLEVEL=logging.DEBUG
logging.basicConfig(format='%(asctime)s [Restore] %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DedupRestore(Delib):

    VERSION = 2019.300 #Year.Yearday

    def __init__(self,dir,host,name):
        logging.info("Datastore directory {}".format(dir))
        self.data = DelibDataDir(dir)
        self.prepareStdOut()


        restore = DelibRestore(data=self.data,host=host,name=name)
        block_cnt = len(restore.db_blocks)
        logging.info("Loaded backup. Have {} blocks".format(block_cnt))
        progress = tqdm(desc=host+"|"+name,total=block_cnt,unit="blocks",leave=False)

        for block in restore:
            block.writeFP(self.raw_out,compressed=False)
            progress.update()

        progress.close()
        logging.info("Done restoring.")



def parse_arguments():
     parser = argparse.ArgumentParser()
     parser.add_argument("--dir",nargs=1,required=True,help="Datablock directory")
     parser.add_argument("--host",nargs=1,required=True,help="Backup host")
     parser.add_argument("--name",nargs=1,required=True,help="Backup name")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    logging.debug("Called: __main__")
    args = parse_arguments()
    logging.info("Starting DedupRestore()")
    dedup = DedupRestore(dir=args.dir[0],host=args.host[0],name=args.name[0])
