import json
import pytz
import re
import uuid
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

def datetime_to_es_format(date):
    return str(date.date())+"T"+str(date.hour)+":"+str(date.minute)+":"+str(date.second)+"Z"

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

def assign_to_cluster(recordList, epsilon, n_min):
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
            self.indexed_at = None
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
            self.indexed_at = d_rec["indexedDate"]
            self.dt = datetime.strptime(d_rec["created_at"],"%Y-%d-%mT%H:%M:%SZ")
            self.cluster = -1
            self.cluster_ind = d_rec["cluster"]

    def toDict(self):
        now = datetime.now()
        obj = None
        if self.indexed_at == None:
            obj = {
                'id': self.id,
                'user': self.username,
                'caption': self.text,
                'tags':self.tags,
                'indexedDate': datetime_to_es_format(now),
                'post_date': datetime_to_es_format(self.dt),
                'location':{
                    "type":"point",
                    "coordinates":[self.lon, self.lat]
                },
                "cluster":self.cluster_ind
            }
        else:
            obj = {
                'id': self.id,
                'user': self.username,
                'caption': self.text,
                'tags':self.tags,
                'indexedDate': datetime_to_es_format(self.indexed_at),
                'post_date': datetime_to_es_format(self.dt),
                'location':{
                    "type":"point",
                    "coordinates":[self.lon, self.lat]
                },
                "cluster":self.cluster_ind
            }
        return obj

    def write_to_es(self, es_index, es_doc_type, es):
        es.index(index=es_index, doc_type=es_doc_type, id=self.id, body=json.dumps(self.toDict()))



class ScoreBin:
    def __init__(self, record=None, hashtag=""):
        self.users = set([])
        self.records = []
        self.n_clusters = -1
        self.tag = hashtag
        if record is not None:
            self.records.append(record)
            self.users.add(record.username)

    def add_record(self, record):
        self.records.append(record)
        self.users.add(record.username)
        #if record.dt < self.dt:

    def to_dict(self):
        return {
            'date': str(self.dt.date()),
            'datetime': str(self.dt),
            'lat': self.lat,
            'lon': self.lon,
            'nUnique': len(self.users),
            'nTotal': len(self.records)
        }

    def cluster_and_write_to_es(self, epsilon, n_min,  es_obj, es_doc_index, es_doc_type, es_clust_index, es_clust_type):
        assign_to_cluster(self.records, epsilon, n_min)
        for no_clust in filter(lambda x: x.cluster == -1, self.records):
            no_clust.write_to_es(es_doc_index, es_doc_type, es_obj)

        clusters_nums = set(map(lambda x: x.cluster, self.records))
        self.n_clusters = len(clusters_nums)
        for num in clusters_nums:
            clustered_recs = sorted(filter(lambda x: x.cluster == num, self.records), key=lambda x: x.dt)
            cluster_users = set(map(lambda x: x.username, clustered_recs))
            cluster_inds = set(map(lambda x: x.cluster_ind, clustered_recs))
            #proceed by case
            #trivial case - cluster found with only existing entries:
            if len(cluster_inds) == 1 and "" not in cluster_inds:
                continue
            #case 1 - new clusters found
            elif len(cluster_inds) == 1:
                cluster_id = uuid.uuid4()
                self.write_cluster_to_es(cluster_id, len(clustered_recs), len(cluster_users), clustered_recs[0], es_obj, es_clust_index, es_clust_type)
                for record in clustered_recs:
                    record.cluster_ind = cluster_id
                    record.write_to_es(es_doc_index, es_doc_type, es_obj)
            #case 2 - new entries associated with existing cluster
            elif len(cluster_inds) == 2:
                c_list = list(cluster_inds)
                cluster_id = c_list[0] if c_list[0] != "" else c_list[1]
                self.write_cluster_to_es(cluster_id, len(clustered_recs), len(cluster_users), clustered_recs[0], es_obj, es_clust_index, es_clust_type, overwrite=True)
                new_recs = filter(lambda x: x.cluster_ind == "", clustered_recs)
                for record in new_recs:
                    record.cluster_ind = cluster_id
                    record.write_to_es(es_doc_index, es_doc_type, es_obj)
            #case 3 - new entries cause existing clusters to merge
            else:
                clusts = []
                for ind in list(cluster_inds):
                    if ind != "":
                        clusts.append(es_obj.get(index=es_clust_index, doc_type=es_clust_type, id=ind))
                clusts = sorted(clusts, key=lambda x: x["_source"]["num_posts"])
                cluster_id = clusts[0]["_id"]
                self.write_cluster_to_es(cluster_id, len(clustered_recs), len(cluster_users), clustered_recs[0], es_obj, es_clust_index, es_clust_type, overwrite=True)
                recs_to_change = filter(lambda x: x.cluster_ind != cluster_id, clustered_recs)
                for record in recs_to_change:
                    record["cluster_ind"] = cluster_id
                    record.write_to_es(es_doc_index, es_doc_type, es_obj)
                #remove old cluster entries, make sure no older posts were missed
                to_remove = map(lambda x: x["_id"], cluster_id[1:])
                for ind in to_remove:
                    es_obj.delete(index=es_clust_index, doc_type=es_clust_type, id=ind)
                    count = es_obj.count(index=es_doc_index, doc_type=es_doc_type, q="cluster:"+ind)["count"]
                    if count > 0:
                        res = es_obj.search(index=es_doc_index, doc_type=es_doc_type, body={"query":{"match":{"cluster":ind}}})
                        for hit in res["hits"]["hits"]:
                            sr = ScoreRecord(hit, data_type=1)
                            sr.cluster_ind = cluster_id
                            sr.write_to_es(es_doc_index, es_doc_type, es_obj)

    def write_cluster_to_es(self, c_id, n_recs, n_users, first_rec, es_obj, es_clust_index, es_clust_type, overwrite=False):
        post_date = datetime_to_es_format(first_rec.dt)
        if overwrite:
            existing_date = datetime.strptime(es_obj.get(index=es_clust_index, doc_type=es_clust_type, id=c_id)["_source"]["post_date"],"%Y-%d-%mT%H:%M:%SZ")
            if existing_date < first_rec.dt:
                post_date = datetime_to_es_format(existing_date)

        now = datetime.now()
        body = {
            "tag":self.tag,
            "post_date":post_date,
            "indexed_date":datetime_to_es_format(now),
            "num_posts":n_recs,
            "num_users":n_users,
            "location":{
                "type":"point",
                "coordinates":[first_rec.lon, first_rec.lat]
            }
        }
        es_obj.index(index=es_clust_index, doc_type=es_clust_type, id=c_id, body=json.dumps(body))
