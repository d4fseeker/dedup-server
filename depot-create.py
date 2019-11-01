"""
Depot-Create - Datastore creator
"""

import argparse,humanfriendly,logging,os       #Helpers
from delib import Delib,DelibDataDir    #Dedup-Server

LOGLEVEL=logging.DEBUG
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DepotCreate(Delib):

    VERSION = 2019.300 #Year.Yearday

    def __init__(self,dir,blocksize_human):
        self.bs = humanfriendly.parse_size(blocksize_human,binary=True)

        logging.info("Datastore blocksize {}".format(self.bs))
        logging.info("Datastore directory {}".format(dir))

        if not os.path.isdir(dir):
            raise Exception("Datadir is not a folder: {}".format(dir))
        if os.listdir(dir):
            raise Exception("Datadir path is not empty: {}".format(dir))
        os.mkdir(dir+"/blocks")
        self.data = DelibDataDir(dir,self.bs)



def parse_arguments():
     parser = argparse.ArgumentParser()
     parser.add_argument("--dir",nargs=1,required=True,help="Datablock directory")
     parser.add_argument("--bs",nargs=1,required=True,help="Human-readable blocksize B|KB|MB|GB|TB")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    logging.debug("Called: __main__")
    args = parse_arguments()
    logging.info("Starting DepotCreate()")
    dedup = DepotCreate(dir=args.dir[0],blocksize_human=args.bs[0])
