import sqlite3,re #Server
import sys,os,stat,io,struct,socket,time,fcntl,glob #Python3 libraries
import xxhash,lz4.frame,tarfile #Dedup
import humanfriendly, logging, math #Helpers
#from tqdm import tqdm #Progress bar

##
## Block handling
##
class DelibBlock:

    @classmethod
    #Load block from compressed variable
    def fromCompressed(cls,data,cblock,hash=None):
        block = lz4.frame.decompress(cblock)
        return cls(data,block,hash)

    @classmethod
    #Load block from filesystem by path
    def fromFile(cls,data,file,compressed):
        with open(file,"rb") as fp:
            block = fp.read()
        if compressed:
            try:
                return cls.fromCompressed(data,cblock=block)
            except RuntimeError as e:
                raise Exception("Decompression failed for {}. {}".format(file,str(e)))
        else:
            return cls(block)


    @classmethod
    #Load block from filesystem by filename
    def fromHash(cls,data,hash):
        try:
            row_block = data.DBGetBlock(hash)
            path = data.dir+"/blocks/"+row_block["filename"]
            return cls.fromFile(data=data,file=path,compressed=row_block["compressed"])
        except Exception as e:
            raise Exception("Failed to fetch block {}. Reason: {}".format(path,str(e)))

    hash = None
    cblock = None

    #Init block
    def __init__(self,data,block,hash=None):
        self.data = data
        self.block = block
        if hash:
            self.hash = hash
        else:
            self.getHash()

    #Save block to disk
    def save(self,compressed,skip_saved=True):
        #Skip existing hashes
        if self.data.hashExists(self.getHash()):
            logging.debug("Skipping existing block %s", block.getHash())
            return False
        #Open file and verify system-wide hash lock
        filename = block.getHash()+".lz4"
        filepath = self.dir+"/blocks/"+filename
        if os.path.exists(filepath):
            raise Exception("Cannot create block file. File already exists: {}".format(filepath))
        block.writeToFile(fp, compressed=True, locked=True)
        #Add to database and optionally commit
        self.cur.execute("INSERT INTO blocks (hash,size,csize,compressed,filename,time_imported) VALUES (:hash,:size,:csize,:compressed,:filename,:time)", {
            "hash": block.getHash(),
            "size": block.getSize(),
            "csize": block.getCompressedSize(),
            "compressed": "lz4",
            "filename": filename ,
            "time": int(time.time())
        })
        if do_commit:
            self.db.commit()
        return True


    #Get block hash. If not defined, calculate
    def getHash(self,update=False):
        if not self.hash or update:
            self.hash = xxhash.xxh64(self.block).hexdigest()
        return self.hash


    #Get uncompressed blocks size
    def getSize(self):
        return len(self.block)


    #Get compressed block
    def getCompressed(self):
        if not self.cblock:
            self.cblock = lz4.frame.compress(self.block)
        return self.cblock


    #Get size of block when compressed
    def getCompressedSize(self):
        return len(self.getCompressed())


    #Write block to given path. Optionally compressed and exclusive-locked
    def writeToFile(self,path,compressed=True,locked=False):
        if os.path.exists(path):
            raise Exception("Cannot write block {}: File exists in path {}".format(self.getHash(),path))
        with open(path,"wb") as fp:
            fcntl.lockf(fp,fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.writeFP(fp,compressed)
        return True


    #Write block to given filepointer. Optionally compressed
    def writeToFP(self,fp,compressed=True):
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

    #Backup States
    STATE_PENDING = "pending"       #Backup is currently being created. Check time_imported to detect stale
    STATE_READY = "ready"           #Backup is completed. time_imported is updated when backup changes to ready
    STATE_FAILED = "failed"         #Backup failed or is stale
    STATE_BROKEN = "broken"         #Backup is affected by data corruption, data loss or other causes for a broken backup
    STATE_DELETED = "deleted"       #Backup has been deleted. Cleanup has not necessarily been run yet!
    ALL_STATES = [ STATE_PENDING, STATE_READY,STATE_FAILED,STATE_BROKEN,STATE_DELETED]

    ##
    ## STATIC
    ##
    @classmethod
    def create(cls,data,host,name,device,time_created):
        data.cur.execute("INSERT INTO backups (name,host,device,time_created,time_imported,state) VALUES (:name,:host,:device,:time_created,:time_imported,:state)",{
            "name": name,
            "host": host,
            "device": device,
            "time_created": time_created,
            "time_imported": int(time.time()),
            "state": cls.STATE_PENDING
        })
        data.db.commit()
        return data.cur.lastrowid

    @classmethod
    def fromName(cls,data,host,name):
        row = data.cur.execute("SELECT rowid FROM backups WHERE host = :host AND name = :name",{"host":host,"name":name}).fetchone()
        if not row:
            raise Exception("No backup with host {} and name {}".format(host,name))
        return cls(data,row["rowid"])


    ##
    ## INSTANCE
    ##

    def __init__(self,data,id,row=None):
        self.data = data
        self.id = id
        if not row:
            self.refreshRow()
        else:
            self.row = row

    #(Re)load from DB
    def refreshRow(self):
        self.row = self.data.cur.execute("SELECT rowid,* FROM backups WHERE rowid = :id",{"id":self.id}).fetchone()
        if not self.row:
            raise Exception("No backup with id {}".format(self.id))
        return self.row

    #Mark a backup with state pending as state ready. Optionally (default) verifies backup database consistency first.
    def finish(self,size,doVerify=True,doReload=True):
        #Verify if backup is STATE_PENDING
        if doReload:
            self.refreshRow()
        if not self.row["state"] == self.STATE_PENDING:
            raise Exception("Cannot mark a backup with state {} as {}".format(self.row["state"],self.STATE_READY))
        #Verify if backup is db-sane
        if doVerify:
            if not self.verify():
                raise Exception("Backup failed verification. Marking as {}".format(self.STATE_FAILED))
        #Mark as done
        self.data.cur.execute("UPDATE backups SET time_imported = :time_imported, state = :state, size = :size WHERE host = :host AND name = :name AND state = :state_pending",{
            "host": host,
            "name": name,
            "size": size,
            "time_imported": int(time.time()),
            "state": self.STATE_READY,
            "state_pending": self.STATE_PENDING
        })
        if self.data.cur.rowcount != 1:
            raise Exception("Unknown error; database failed to update. Wrong state or backup does not exist?")
        self.data.db.commit()


    #Returns a backup size if the backup size has already been defined. Otherwise returns null
    def getSize(self):
        return self.row["size"]


    #Get backup ID
    def getId(self):
        return self.row["rowid"]


    #Verify backup db continuity and size by checking:
    # - Continuity
    # - Length
    # - If all referenced blocks exist in DB (Not on disk!)
    def verify_continuity(self,size=None,throw_exception=True):
        has_err = False
        if not size:
            size = self.getSize()
        #Iterate through database
        iter = self.data.cur.execute("SELECT bl.hash,bb.pos,bb.block,ba.rowid FROM backups ba LEFT JOIN backup_blocks bb ON ba.rowid = bb.backup LEFT JOIN blocks bl ON bb.block = bl.hash WHERE ba.rowid = :backup_id ORDER BY bb.pos ASC",{"backup_id":self.getId()})
        expect_pos = 1
        for row in iter:
            if not row["hash"]:
                logging.error("Backup %s misses block %s on pos %d",self.getId(),row["block"],expect_pos)
                has_err = True
            if row["pos"] != expect_pos:
                logging.error("Backup %s misses pos %s",self.getId(),expect_pos)
                has_err = True
            expect_pos += 1
        expect_size = (expect_pos-1) * self.data.getBlocksize()
        if expect_size != size:
            has_err = True
            logging.error("Backup is shorter than expected. Is %d, should be %d",expect_size,size)

        if throw_exception:
            raise Exception("Backup {} is damaged!".format(self.getId()))
        return not has_err


    #Link a hash to this backup at position pos
    def link(self,pos,hash,do_commit=True):
        if not hash:
            raise Exception("Hash is not defined")
        self.data.cur.execute("INSERT INTO backup_blocks (pos,block,backup) VALUES ( :pos , :block , :backup )", { "pos": pos, "backup": backup, "block": hash })
        if do_commit:
            self.data.db.commit()

    #Get hashes in a backup in a ordered list
    def getHashes(self):
        list = []
        self.data.cur.execute("SELECT block FROM backup_blocks WHERE backup = :backup ORDER BY pos ASC", { "backup": self.id })
        for row in self.cur:
            list.append(row)
        return list

    #Iterate over the backup
    def __iter__(self):
        return DelibBackupIterator(self)


class DelibBackupIterator:

    def __init__(self,backup):
        self.backup = backup
        self.hashes = self.backup.getHashes()
        self._index = 0
        self._lastid = len(self.hashes)

    def __next__(self):
        if self._index >= self._lastid:
            raise StopIteration
        block = DelibBlock.fromHash(self.backup.data,self.hashes[self._index])
        self._index += 1
        return block


class DelibDataDir:

    #Database name to expect inside data dir
    DB_NAME = "db.sqlite3"

    #Create a new datadir
    @classmethod
    def create(cls,dir,blocksize):
        #
        # Create files and folders
        #
        if not os.path.isdir(dir):
            raise Exception("Datadir is not a folder: {}".format(dir))
        if os.listdir(dir):
            raise Exception("Datadir path is not empty: {}".format(dir))
        os.mkdir(dir+"/blocks")
        os.mkdir(dir+"/damaged")
        #
        #Create database
        #
        db_path = dir+"/"+cls.DB_NAME
        #Pre-run Sanity check
        if os.path.isfile(db_path):
            raise Exception("Cannot create datastore: already exists in {}".format(db_path))
        #Open/Create database
        db = sqlite3.connect(db_path)
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        #Create
        logging.info("Creating database")
        logging.debug("Creating table settings")
        cur.execute("CREATE TABLE settings(key TEXT, value TEXT)")
        #Blocks
        logging.debug("Creating table blocks")
        cur.execute("CREATE TABLE blocks(hash TEXT PRIMARY KEY ,size INTEGER,csize INTEGER, compressed TEXT, filename TEXT, time_imported INTEGER)")
        #Backups
        logging.debug("Creating table backups")
        cur.execute("CREATE TABLE backups(name TEXT, host TEXT, device TEXT, size INTEGER, time_created INTEGER, time_imported INTEGER, state TEXT CHECK( state IN ('pending','ready','failed','broken','deleted') ), UNIQUE(host,name) ) ")
        #Backup->Blocks
        logging.debug("Creating table backup_blocks")
        cur.execute("CREATE TABLE backup_blocks(pos INTEGER,  block NOT NULL REFERENCES blocks, backup NOT NULL REFERENCES backups)")
        #Data and commit
        cur.execute("INSERT INTO settings(key,value) VALUES ('blocksize',:blocksize)",{"blocksize":blocksize})
        db.commit()
        logging.info("Done creating database")
        #Return created datadir
        return cls(dir)


    def __init__(self,dir):
        self.dir = dir
        self.settings = {}
        self._DBOpen()

    #Return defined blocksize
    def getBlocksize(self):
        return int(self.settings["blocksize"])

    #Check whether given hash exists in database
    def hashExists(self,myhash):
        return ( self.cur.execute("SELECT COUNT(rowid) FROM blocks WHERE hash = :hash",{"hash": myhash}).fetchone()[0] > 0 )


    #Get list of all hashes in DB
    def getHashes(self):
        hashes = []
        self.cur.execute("SELECT hash FROM blocks ORDER BY hash ASC")
        for row in self.cur:
            hashes.append(row["hash"])
        return hashes

    #Get list of tuples {host,name} from database, limited by provided state (all states by default)
    def getBackupsByState(self,state=DelibBackup.ALL_STATES):
        return self.cur.execute("SELECT host,name FROM backups WHERE state = :state",{"state":state}).fetchall()

    #Get 2d-dict of state -> tuples {host,name} from database
    def getBackups(self):
        backups = {}
        for state in DelibBackup.ALL_STATES:
            backups[state] = self.getBackupsByState(state)
        return backups

    ##
    ## DATABASE-commands
    ##

    #Get full row for block from DB
    def DBGetBlock(self,hash):
        row = self.cur.execute("SELECT * FROM blocks WHERE hash = :hash",{ "hash": hash }).fetchone()
        if not row:
            raise Exception("No such block in database: {}".format(hash))
        return row

    #Get full row for all blocks from DB
    def DBGetBlocks(self,hash):
        rows = []
        self.cur.execute("SELECT * FROM blocks ORDER BY hash ASC",{ "hash": hash })
        for row in self.cur:
            rows.append(row)
        return rows

    #Get filename for hash from DB
    def DBGetFilename(self,hash):
        return self.DBGetBlock(hash)["filename"]


    #Open database
    def _DBOpen(self):
        db_path = self.dir+"/"+self.DB_NAME
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

    ##
    ## BADBLOCK Management
    # Bad blocks have a filename <HASH>.<EXTENSION>.<TIMESTAMP-EPOCH>.broken
    #
    # Note that several versions of a broken block could be in the folder over time;
    # either with the same or different content
    ##

    #Get full filename of all broken hashes.
    def BBgetFiles(self):
        return glob.glob(self.dir+"/damaged/*.broken")

    #Return only broken hashes. Warning: a non-broken hash may exist in blocks folder!
    def BBgetHashes(self):
        hashes = []
        for file in self.BBgetFiles():
            hash = re.search("^\w+",file)
            hashes.append(hash)
        return hashes


class Delib:

    tar = {}
    TAR_HEADERS = [ "/backup/host", "/backup/device", "/backup/blocksize", "/backup/filesize", "/backup/created", "/dedup/version" ]
    TAR_FOOTERS = [ "/backup/list" ]

    host = None
    name = None
    data = None

    #Datadir handling
    def getData(self,dir=None):
        logging.info("Datastore directory %s",dir)
        if dir:
            self.dir = dir
        if not self.data and not self.dir:
            raise Exception("dir must be defined on first getData()")
        if not self.data:
            self.data = DelibDataDir(dir)
        return self.data

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
            logging.debug("Config: %s = %s",k,"[...]")
        else:
            logging.debug("Config: %s = %s",k,v)
        self.tar[k] = v
        return k,v


    def verifyTarHeaders(self):
        if self.tar["backup_blocksize"] != self.data.getBlocksize():
            raise Exception("Tar blocksize {} differs from datastore blocksize {}".format(self.tar["backup_blocksize"],self.data.bs))
        logging.debug("Verified backup blocksize %s ok",self.tar["backup_blocksize"])
