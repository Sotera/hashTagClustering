import json
import pytz
import re
import elasticsearch
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from math import sqrt

def utc_to_local(utc_dt):
    # get integer timestamp to avoid precision lost
    local_tz = pytz.timezone('US/Eastern')
    local_dt = local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    assert utc_dt.resolution >= timedelta(microseconds=1)
    return local_dt.replace(microsecond=utc_dt.microsecond)

def text_to_hashtags(caption):
    term_list = re.sub('[^\w\s#]', '', caption, flags=re.UNICODE).lower().split()
    ret_list = set()
    for term in term_list:
        try:
            if term[0]=="#":
                if term.find("#",1)==-1:
                    if len(term) > 2:
                        ret_list.add(term[1:])
                else:
                    subs = term.split("#")
                    for sub in subs:
                        if len(sub) > 2:
                            ret_list.add(sub)
        except:
            continue
    return list(ret_list)

def assignToCluster(recordList, epsilon, n_min):
    lalo = []
    for line in recordList:
        lalo.append([line.lon, line.lat])

    X = StandardScaler().fit_transform(lalo)
    fitObj = StandardScaler().fit(lalo)
    laEps = epsilon/fitObj.std_[0]
    loEps = epsilon/fitObj.std_[1]
    fitEps = sqrt(laEps*laEps+loEps*loEps)
    db = DBSCAN(eps=fitEps, min_samples=n_min).fit(X)
    for ind in range(len(recordList)):
        recordList[ind].cluster = db.labels_[ind]

class ScoreRecord:
    def __init__(self, json_data, data_type=0):
        if data_type==0:
            record = json.loads(json_data)
            self.id = record["id_str"]
            self.lat = record["geo"]["coordinates"][0]
            self.lon = record["geo"]["coordinates"][1]
            self.text = record["text"]
            self.username = record["user"]["screen_name"]
            self.tags = text_to_hashtags(record["text"])
            self.dt = utc_to_local(datetime.strptime(record["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
            self.cluster = -1
            self.cluster_ind = ""
        elif data_type==1:
            d_rec = json_data["_source"]
            self.id = d_rec["id"]
            self.lat = d_rec["location"]["coordinates"][1]
            self.lon = d_rec["location"]["coordinates"][0]
            self.text = d_rec["caption"]
            self.username = d_rec["user"]
            self.tags = d_rec["tags"]
            self.dt = datetime.strptime(d_rec["created_at"],"%Y-%d-%mT%H:%M:%SZ")
            self.cluster = -1
            self.cluster_ind = d_rec["cluster"]

    def toDict(self):
        obj = {
            'id': self.id,
            'user': self.username,
            'caption': self.text,
            'tags':self.tags,
            'post_date': str(self.dt.date())+"T"+str(self.dt.hour)+":"+str(self.dt.minute)+":"+str(self.dt.second)+"Z",
            'location':{
                "type":"point",
                "coordinates":[self.lon, self.lat]
            },
            "cluster":self.cluster_ind
        }
        return obj

    def write_to_es(self, es_index, es_doc_type, es):
        mapped = self.toDict()
        es.index(index=es_index, doc_type=es_doc_type, id=self.id, body=json.dumps(mapped))



class ScoreBin:
    def __init__(self, record=None):
        self.users = set([])
        self.records = []
        self.n_clusters = -1
        self.clusters = []
        if record is not None:
            self.records.append(record)
            self.users.add(record.username)

    def addRecord(self, record):
        self.records.append(record)
        self.users.add(record.username)
        #if record.dt < self.dt:
        #    self.dt = record.dt

    def toDict(self):
        return {
            'date': str(self.dt.date()),
            'datetime': str(self.dt),
            'lat': self.lat,
            'lon': self.lon,
            'nUnique': len(self.users),
            'nTotal': len(self.records)
        }

    def determine_clusters(self, epsilon, n_min):
        assignToCluster(self.records, epsilon, n_min)
        #clustered_recs = filter(lambda x: x.cluster != -1, self.records)
        #cluster_nums = set()
        #for record in self.records:
