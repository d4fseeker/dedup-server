import sqlite3,re #Server
import sys,os,stat,io,struct,socket,time,fcntl #Python3 libraries
import xxhash,lz4.frame,tarfile #Dedup
import humanfriendly, logging, math #Helpers
#from tqdm import tqdm #Progress bar

##
## Block handling
##
class DelibBlock:

    @classmethod
    def fromCompressed(cls,cblock,hash=None):
        block = lz4.frame.decompress(cblock)
        return cls(block,hash)

    @classmethod
    def fromFile(cls,file,compressed):
        with open(file,"rb") as fp:
            block = fp.read()
        if compressed:
            try:
                return cls.fromCompressed(block)
            except RuntimeError as e:
                raise Exception("Decompression failed for {}. {}".format(file,str(e)))
        else:
            return cls(block)

    def __init__(self,block,hash=None):
        self.block = block
        if hash:
            self.hash = hash
        else:
            self.getHash()

    hash = None
    def getHash(self,update=False):
        if not self.hash or update:
            self.hash = xxhash.xxh64(self.block).hexdigest()
        return self.hash

    def getSize(self):
        return len(self.block)

    cblock = None
    def getCompressed(self):
        if not self.cblock:
            self.cblock = lz4.frame.compress(self.block)
        return self.cblock

    def getCompressedSize(self):
        return len(self.getCompressed())

    def writeTo(self,path,compressed=True):
        if os.path.exists(path):
            raise Exception("Cannot write block {}: File exists in path {}".format(self.getHash(),path))
        with open(path,"wb") as fp:
            self.writeFP(fp,compressed)
        return True

    def writeFP(self,fp,compressed=True):
        if compressed:
            fp.write(self.getCompressed())
        else:
            fp.write(self.block)
        return True






###
### DATASTORE
###

#States
## pending: backup is being created. Possibility to check stale/crashed backups based on time_imported
## ready: backup is fully functional. time_imported is updated after complete import
## failed: backup is stale during pending or crashed
## broken: datastore sanity check detected data inconsistenices affecting this backup
## deleted: backup got deleted

class DelibBackup:

    def __init__(self,data,host,name,device,time_created):
        self.host = host
        self.name = name
        self.data = data
        self.id = self.data._DBCreateBackup(host=host,name=name,device=device,time_created=time_created)

    def finish(self,size):
        self.data._DBFinishBackup(host=self.host,name=self.name,size=size)

    def link(self,pos,hash,do_commit=True):
        if not hash:
            raise Exception("Hash is not defined")
        self.data._DBLinkBackupHash(self.id,hash,pos,do_commit=do_commit)

class DelibRestore:

    def __init__(self,data,host,name):
        self.data = data
        self.name = name
        self.host = host
        backup_id = self.data._DBGetBackupId(host,name)
        self.db_blocks = self.data._DBGetBackupBlocks(backup_id)

    def __iter__(self):
        return DelibRestoreIterator(self)

class DelibRestoreIterator:

    def __init__(self,restore):
        self.restore = restore
        self._index = 0
        self._lastid = len(self.restore.db_blocks)

    def __next__(self):
        if self._index >= self._lastid:
            raise StopIteration
        row = self.restore.db_blocks[self._index]
        path = self.restore.data.dir + "/blocks/" + row["filename"]
        self._index += 1
        is_compressed = bool(len(row["compressed"]))
        return DelibBlock.fromFile(path,compressed=is_compressed)





class DelibDataDir:

    STATE_PENDING = "pending"       #Backup is currently being created. Check time_imported to detect stale
    STATE_READY = "ready"           #Backup is completed. time_imported is updated when backup changes to ready
    STATE_FAILED = "failed"         #Backup failed or is stale
    STATE_BROKEN = "broken"         #Backup is affected by data corruption, data loss or other causes for a broken backup
    STATE_DELETED = "deleted"       #Backup has been deleted. Cleanup has not necessarily been run yet!

    NAME_DB = "db.sqlite3"

    def __init__(self,dir,create_blocksize=False):
        self.dir = dir
        self.settings = {}
        if create_blocksize:
            self._DBCreate(create_blocksize)
        else:
            self._DBOpen()

    def getBlocksize(self):
        return self.settings["blocksize"]

    def addBlock(self,block,do_commit=True):
        if not isinstance(block,DelibBlock):
            raise TypeError("Must be DelibBlock, not {}".format(type(block)))
        #Skip existing hashes
        if self._DBHashExists(block.getHash()):
            logging.debug("Skipping existing block {}".format(block.getHash()))
            return False
        #Open file and verify system-wide hash lock
        filename = block.getHash()+".lz4"
        filepath = self.dir+"/blocks/"+filename
        if os.path.exists(filepath):
            raise Exception("Cannot create block file. File already exists: {}".format(filepath))
        with open(filepath,"wb") as fp:
            fcntl.lockf(fp,fcntl.LOCK_EX | fcntl.LOCK_NB)
            #Write hash
            block.writeFP(fp, compressed=True)
            self._DBAddBlock(filename,block,do_commit=do_commit)
        return True

    def getBlockByHash(self,hash):
        row_block = self._DBGetBlock(hash)
        with open(self.dir+"/blocks/"+row_block["filename"]) as fp:
            rawblock = fp.read()
        if row_block["compressed"]:
            block = DelibBlock.fromCompressed(rawblock,hash)
        else:
            block = DelibBlock(rawblock,hash)
        return block

    def removeBlockByHash(self,hash):
        ## TODO: implement later
        pass

    def hashExists(self,hash):
        #Proxy for self._DBHashExists()
        return self._DBHashExists(hash)


    ##
    ## DATABASE-specific backend
    ## Override for other database engines
    ##

    def _DBAddBlock(self,filename,block,do_commit=True):
        self.cur.execute("INSERT INTO blocks (hash,size,csize,compressed,filename,time_imported) VALUES (:hash,:size,:csize,:compressed,:filename,:time)", {
            "hash": block.getHash(),
            "size": block.getSize(),
            "csize": block.getCompressedSize(),
            "compressed": "lz4",
            "filename": filename ,
            "time": int(time.time())
        })
        if do_commit:
            self._DBCommit()

    def _DBGetBlock(self,hash):
        row = self.cur.execute("SELECT * FROM blocks WHERE hash = :hash",{ "hash": hash }).fetchone()
        if not row:
            raise Exception("No such block in database: {}".format(hash))
        return row

    def _DBCreateBackup(self,host,name,device,time_created):
        self.cur.execute("INSERT INTO backups (name,host,device,time_created,time_imported,state) VALUES (:name,:host,:device,:time_created,:time_imported,:state)",{
            "name": name,
            "host": host,
            "device": device,
            "time_created": time_created,
            "time_imported": int(time.time()),
            "state": self.STATE_PENDING
        })
        self.db.commit()
        return self.cur.lastrowid

    def _DBFinishBackup(self,host,name,size):
        self._DBVerifyBackup(host=host,name=name)
        self.cur.execute("UPDATE backups SET time_imported = :time_imported, state = :state, size = :size WHERE host = :host AND name = :name",{
            "host": host,
            "name": name,
            "size": size,
            "time_imported": int(time.time()),
            "state": self.STATE_READY
        })
        self.db.commit()

    #Check if:
    # - backup size corresponds to blocks * blocksize
    # - blocks are continous and start at 1
    def _DBVerifyBackup(self,host,name):
        #TODO
        pass


    def _DBGetBackupHashes(self,backup):
        list = []
        self.cur.execute("SELECT block FROM backup_blocks WHERE backup = :backup ORDER BY pos ASC", { "backup": backup })
        for row in self.cur:
            list.append(row)
        return list

    def _DBGetBackupBlocks(self,backup):
        list = []
        return self.cur.execute("SELECT b.*,bb.pos FROM backup_blocks bb LEFT JOIN blocks b ON bb.block = b.hash ORDER BY bb.pos ASC").fetchall()

    def _DBGetBackupId(self,host,name):
        res = self.cur.execute("SELECT ROWID FROM backups WHERE host = :host AND name = :name",{ "host": host, "name": name }).fetchone()
        if not res:
            raise Exception("No backup with host {} and name {}".format(host,name))
        return res["ROWID"]

    def _DBLinkBackupHash(self,backup,hash,pos,do_commit=True):
        self.cur.execute("INSERT INTO backup_blocks (pos,block,backup) VALUES ( :pos , :block , :backup )", { "pos": pos, "backup": backup, "block": hash })
        if do_commit:
            self._DBCommit()

    def _DBHashExists(self,myhash):
        return ( self.cur.execute("SELECT COUNT(rowid) FROM blocks WHERE hash = :hash",{"hash": myhash}).fetchone()[0] > 0 )

    def _DBHashList(self):
        hashes = []
        self.cur.execute("SELECT hash FROM blocks ORDER BY hash ASC")
        for row in self.cur:
            hashes.append(row["hash"])
        return hashes

    def _DBCommit(self):
        self.db.commit()


    def _DBOpen(self):
        db_path = self.dir+"/"+self.NAME_DB
        #Pre-run Sanity check
        if not os.path.isfile(db_path):
            raise Exception("Cannot open datastore: does not exist in {}".format(db_path))
        #Open/Create database
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        #self.db.set_trace_callback(logging.debug) #DEBUG DB
        self.cur = self.db.cursor()
        #Load settings
        for row in self.db.execute("SELECT key,value FROM settings"):
            self.settings[row["key"]] = row["value"]

    def _DBCreate(self,blocksize):
        db_path = self.dir+"/"+self.NAME_DB
        #Pre-run Sanity check
        if os.path.isfile(db_path):
            raise Exception("Cannot create datastore: already exists in {}".format(db_path))
        #Open/Create database
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self.cur = self.db.cursor()
        #Create
        logging.info("Creating database")
        logging.debug("Creating table settings")
        self.cur.execute("CREATE TABLE settings(key TEXT, value TEXT)")
        #Blocks
        logging.debug("Creating table blocks")
        self.cur.execute("CREATE TABLE blocks(hash TEXT PRIMARY KEY ,size INTEGER,csize INTEGER, compressed TEXT, filename TEXT, time_imported INTEGER)")
        #Backups
        logging.debug("Creating table backups")
        self.cur.execute("CREATE TABLE backups(name TEXT, host TEXT, device TEXT, size INTEGER, time_created INTEGER, time_imported INTEGER, state TEXT CHECK( state IN ('pending','ready','failed','broken','deleted') ), UNIQUE(host,name) ) ")
        #Backup->Blocks
        logging.debug("Creating table backup_blocks")
        self.cur.execute("CREATE TABLE backup_blocks(pos INTEGER,  block NOT NULL REFERENCES blocks, backup NOT NULL REFERENCES backups)")
        #Data and commit
        self.cur.execute("INSERT INTO settings(key,value) VALUES ('blocksize',{});".format(blocksize))
        self.db.commit()
        logging.info("Done creating database")


class Delib:

    tar = {}
    TAR_HEADERS = [ "/backup/host", "/backup/device", "/backup/blocksize", "/backup/filesize", "/backup/created", "/dedup/version" ]
    TAR_FOOTERS = [ "/backup/list" ]

    host = None
    name = None

    def __init__(self,datadir,host=None,name=None):
        self.host = host
        self.name = name
        if not isinstance(datadir,DelibDataDir):
            raise TypeError("Must be type DelibDataDir, not {}".format(type(datadir)))
        self.data = datadir

    ##
    ## STDio handling
    ##


    def prepareStdOut(self):
        self.raw_out = self._prepareStdIO(sys.stdout)

    def prepareStdin(self):
        self.raw_in = self._prepareStdIO(sys.stdin,isWrite=False)

    def _prepareStdIO(self,stream,isWrite=True):
        if sys.platform == "win32": #Fix for Windows, see https://stackoverflow.com/questions/2374427/python-2-x-write-binary-output-to-stdout
            logging.debug("Applying windows binary input")
            import msvcrt
            msvcrt.setmode(sys.stdin.fileno(), os.O_BINARY)
            return sys.stdin
        else:
            logging.debug("Applying unix binary input")
            if isWrite:
                return os.fdopen(stream.fileno(), "wb", closefd=False)
            else:
                return os.fdopen(stream.fileno(), "rb", closefd=False)



    ##
    ## TAR handling
    ##

    def extractTarHeader(self,tarinfo,need_headers):
        if tarinfo.name not in need_headers:
            raise Exception("Unexpected entry while processing TAR-config: {}".format(tarinfo.name))
        k = tarinfo.name.lstrip("/").replace("/","_")
        fp = self.fp.extractfile(tarinfo)
        v = fp.read().decode("utf-8")
        if k == "backup_list":
            logging.debug("Config: {} = {}".format(k,"[...]"))
        else:
            logging.debug("Config: {} = {}".format(k,v))
        self.tar[k] = v
        return k,v


    def verifyTarHeaders(self):
        if self.tar["backup_blocksize"] != self.data.getBlocksize():
            raise Exception("Tar blocksize {} differs from datastore blocksize {}".format(self.tar["backup_blocksize"],self.data.bs))
        logging.debug("Verified backup blocksize {} ok".format(self.tar["backup_blocksize"]))



    ##
    ## DATASTORE handling
    ##

    data = None
    def loadDataStore(self,dir):
        if self.data:
            raise Exception("Datastore already loaded")
        self.data = DelibData(dir)
        return self.data

    def createDataStore(self,dir,bs):
        if self._datastore:
            raise Exception("Datastore already created")
        self.data = DelibData(dir,bs)
        return self.data
