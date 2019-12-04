#!/usr/bin/env python3

"""
Depot-Create - Datastore creator
"""

import argparse,argparse_logging,humanfriendly,logging,os       #Helpers
from delib import Delib,DelibDataDir    #Dedup-Server

LOGLEVEL=logging.DEBUG
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DepotCreate(Delib):

    VERSION = 2019.337 #Year.Yearday

    def __init__(self,dir,bs):
        bs_byte = humanfriendly.parse_size(bs,binary=True)
        DelibDataDir.create(dir=dir,blocksize=bs_byte)



def parse_arguments():
     parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,description=
        "Creates a new datastore on an existing, empty folder."
     )
     argparse_logging.add_log_level_argument(parser)
     parser.add_argument("-d","--dir",required=True,help="Datablock directory")
     parser.add_argument("-b","--blocksize",required=True,help="Human-readable blocksize in 1024-notation. Suffixes: B|KB|MB|GB|TB")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    LOGLEVEL=logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')

    args = parse_arguments()
    DepotCreate(dir=args.dir,bs=args.blocksize)
