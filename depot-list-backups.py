"""
Depot-Create - Datastore hash list
"""

import argparse,humanfriendly,logging,os,datetime,json       #Helpers
from delib import Delib,DelibDataDir    #Dedup-Server

LOGLEVEL=logging.DEBUG
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=LOGLEVEL, datefmt='%Y-%m-%d %H:%M:%S')


class DepotListBackups(Delib):

    VERSION = 2019.300 #Year.Yearday

    def __init__(self,dir,state,host,format):
        logging.info("Datastore directory {}".format(dir))
        self.data = DelibDataDir(dir)

        if format not in ("cli","csv","json"):
            raise Exception("Unsupported format {}. Must be csv or json".format(format))

        if not host and state == "all":
            self.data.cur.execute("SELECT * FROM backups")
        elif not host:
            self.data.cur.execute("SELECT * FROM backups WHERE state = :state",{ "state": state})
        elif state == "all":
            self.data.cur.execute("SELECT * FROM backups WHERE host = :host",{"host": host})
        else:
            self.data.cur.execute("SELECT * FROM backups WHERE host = :host and state = :state",{"host":host,"state":state})

        data = []
        if format == "csv":
            print("HOSTNAME|BACKUP_NAME|BACKUP_CREATED")
        elif format == "cli":
            print("HOSTNAME".ljust(26)+" | "+"BACKUP_NAME".ljust(26)+" | "+"DATE_CREATED".ljust(16)+"\n"+("-"*80))
        for row in self.data.cur:
            time_str = datetime.datetime.fromtimestamp(row["time_created"]).strftime("%Y-%m-%d_%H-%M-%S")
            if format == "csv":
                print("{}|{}|{}".format(row["host"],row["name"],time_str))
            elif format == "cli":
                print("{} | {} | {}".format(row["host"].ljust(26),row["name"].ljust(26),time_str.ljust(26)))
            else:
                rowdict = {}
                for k in row.keys():
                    rowdict[k] = row[k]
                data.append(rowdict)

        if format == "json":
            print(json.dumps(data))






def parse_arguments():
     parser = argparse.ArgumentParser()
     parser.add_argument("--dir",nargs=1,required=True,help="Datablock directory")
     parser.add_argument("--host",nargs=1,required=False,default=[False],help="Limit to given host")
     parser.add_argument("--state",nargs=1,required=False,default=["ready"],help="Show backups in this state. Options=ready|all|pending|failed|deleted|broken Default=ready")
     parser.add_argument("--format",nargs=1,required=False,default=["cli"],help="Output format. Options=cli,csv,json Default=cli")
     args = parser.parse_args()
     return args


if __name__ == "__main__":
    logging.debug("Called: __main__")
    args = parse_arguments()
    logging.info("Starting DepotListBackups()")
    dedup = DepotListBackups(dir=args.dir[0],state=args.state[0],host=args.host[0],format=args.format[0])
