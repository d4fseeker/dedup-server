"""
Depot - Dedup 2 Datastore
Known Issues:
    -   If a block is not provided but not referenced, depot doesnt detect.
        Should be handled by database constraint. Why not working?
"""

import argparse, logging    #Helpers
import lz4.frame,tarfile,re            #Dedup
from delib import Delib,DelibBlock,DelibDataDir,DelibBackup   #Dedup-Server

LOGLEVEL=logging.INFO
logging.basicConfig(format='%(asctime)s [Depot] %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class Depot(Delib):

    VERSION = 2019.300 #Year.Yearday

    SKIP_KNOWN_BLOCKS_ENTIRELY = True   #Skips verifying a known-blocks hash
    SKIP_VERIFYING_BLOCKS = True        #Skips verifying if a block actually has the given hash
                                        #WARNING: Turning on SKIP_VERIFYING_BLOCKS will prevent trasport corruption or malformed blocks from being detected!
    DELAY_DB_BLOCK_COMMIT = True        #Runs a single commit at the end for all new blocks
    DELAY_DB_LINK_COMMIT = True         #Runs a single commit at the end for all backup links

    STATE_HEADER = 1
    STATE_BODY = 2
    STATE_FOOTER = 3
    STATE_DONE = 4

    def __init__(self,dir_path,host,name):
        dir = DelibDataDir(dir_path)
        Delib.__init__(self, dir, host, name)
        #Prepare reading
        self.prepareStdin()

    def process(self):

        self.state =self.STATE_HEADER
        self.tar = {}
        self.need_headers = self.TAR_HEADERS.copy()
        self.need_footers = self.TAR_FOOTERS.copy()
        logging.info("Starting TAR read")

        with tarfile.open(mode='r|', fileobj=self.raw_in) as self.fp:
            for tarinfo in self.fp:
                ##
                ## HEADERS
                ##
                if self.state == self.STATE_HEADER:
                    k,v = self.extractTarHeader(tarinfo,self.need_headers)
                    self.need_headers.remove(tarinfo.name)
                    logging.debug("Got tar header {}. {} remaining".format(k,len(self.need_headers)))
                    if len(self.need_headers) == 0:
                        logging.info("TAR-header done")
                        self.state += 1
                        #Create backup "session"
                        self.backup = DelibBackup(data=self.data,host=self.host,name=self.name,device=self.tar["backup_device"],time_created=self.tar["backup_created"])
                        continue

                ##
                ## BODY/Blocks
                ##
                if self.state == self.STATE_BODY:
                    #Check if we reached footer
                    matches = re.search("^\/newblocks\/([a-zA-Z0-9]{1,})\.(lz4|tar)$",tarinfo.name)
                    if not matches:
                        #Commit body blocks before continuing
                        if self.DELAY_DB_BLOCK_COMMIT:
                            self.data._DBCommit()
                        logging.info("TAR-body done")
                        self.state += 1
                    else:
                        client_hash = matches.group(1)
                        #logging.debug("Processing new block {}".format(client_hash))

                        if not ( self.SKIP_KNOWN_BLOCKS_ENTIRELY and self.data.hashExists(client_hash) ):
                            #Extract
                            block = self.fp.extractfile(tarinfo)
                            block = block.read()
                            if matches.group(2) == "lz4":
                                block = lz4.frame.decompress(block)
                            if self.SKIP_VERIFYING_BLOCKS:
                                block = DelibBlock(block,client_hash)
                            else:
                                block = DelibBlock(block)
                            #Verify transfer with hash
                            if client_hash != block.getHash():
                                raise Exception("Client hash {} differs from server hash {} for block {}".format(client_hash,block.getHash(),tarinfo.name))
                            #Store block
                            self.data.addBlock(block,do_commit=(not self.DELAY_DB_BLOCK_COMMIT))
                        else:
                            #logging.debug("Skipping known block {} entirely. Fast mode".format(client_hash))
                            pass

                ##
                ## FOOTERS
                ##
                if self.state == self.STATE_FOOTER:
                    logging.info("TAR Footer: {}".format(tarinfo.name))
                    k,v = self.extractTarHeader(tarinfo,self.need_footers)
                    self.need_footers.remove(tarinfo.name)
                    if len(self.need_footers) == 0:
                        logging.info("TAR complete. Linking backup.")
                        self.state += 1
                        #Add backup links
                        hash_pos = 1
                        for myhash in self.tar["backup_list"].splitlines():
                            #logging.debug("Linking hash {}".format(myhash))
                            self.backup.link(hash_pos,myhash,do_commit=(not self.DELAY_DB_LINK_COMMIT))
                            hash_pos += 1
                        #Finish backup
                        self.backup.data._DBCommit()
                        self.backup.finish(size=self.tar["backup_filesize"])
                        logging.info("Backup linked and finished.")
                        break


        if not self.state == self.STATE_DONE:
            raise Exception("TAR incomplete. Corrupted or incomplete backup? Current state: {}".format(self.state))
        else:
            logging.info("Done processing")



def parse_arguments():
     parser = argparse.ArgumentParser()
     parser.add_argument("--dir",nargs=1,required=True,help="Datablock directory")
     parser.add_argument("--host",nargs=1,required=True,help="Client hostname")
     parser.add_argument("--name",nargs=1,required=True,help="Backup name")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    logging.debug("Called: __main__")
    args = parse_arguments()
    logging.info("Starting Depot()")
    host = getattr(args,"host",[None])
    name = getattr(args,"name",[None])
    depot = Depot(dir_path=args.dir[0],host=host[0],name=name[0])
    depot.process()
