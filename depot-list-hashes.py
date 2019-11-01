"""
Depot-List-Hashes - Datastore hash list
"""

import argparse,humanfriendly,logging,os       #Helpers
from delib import Delib,DelibDataDir    #Dedup-Server

LOGLEVEL=logging.INFO
logging.basicConfig(format='%(asctime)s [Hashes] %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DepotList(Delib):

    VERSION = 2019.300 #Year.Yearday

    def __init__(self,dir):
        logging.info("Datastore directory {}".format(dir))
        self.data = DelibDataDir(dir)
        for hash in self.data._DBHashList():
            print(hash)



def parse_arguments():
     parser = argparse.ArgumentParser()
     parser.add_argument("--dir",nargs=1,required=True,help="Datablock directory")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    logging.debug("Called: __main__")
    args = parse_arguments()
    logging.info("Starting DepotList()")
    dedup = DepotList(dir=args.dir[0])
