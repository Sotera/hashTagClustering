import os
import sys
import elasticsearch
sys.path.insert(0, './lib/')
from clustering import ScoreRecord, ScoreBin, assign_to_cluster

def analyze_recent(tweet_file_path, es_url=None, tag_blacklist=[]):
    es = None
    if es_url == None:
        es = elasticsearch.Elasticsearch()
    else:
        es = elasticsearch.Elasticsearch([es_url])

    files = sorted(os.listdir(tweet_file_path), key=lambda x: os.stat(os.path.join(tweet_file_path, x)).st_mtime)
    new_records = {}
    for file in files:
        print "analyzing file:", file
        d0 = open(tweet_file_path + "/" + file)
        for line in d0:
            sr = ScoreRecord(line)
            for tag in sr.tags:
                if tag in new_records.keys():
                    new_records[tag].append(sr)
                else:
                    new_records[tag] = [sr]
        os.rename(tweet_file_path+"/"+file, tweet_file_path+"/analyzed/"+file)
        break

    print len(new_records.keys())

    for tag, lst_rec in new_records.iteritems():
        print "Getting data for tag: ", tag,
        count = es.count(index="jag_hc2_documents", doc_type="post", q="tags:"+tag)["count"]
        #test if there is enough entries for clustering
        n_entries = count+len(lst_rec)
        if n_entries<5:
            print tag, "has only", n_entries, "entries (insufficient for clustering)"
            for sr in lst_rec:
                sr.write_to_es("jag_hc2_documents","post",es)
            continue

        #associate querries with the existing hashtag list
        first = True
        tag_bin = None
        for entry in lst_rec:
            if first:
                first = False
                tag_bin = ScoreBin(record=entry, hashtag=tag)
            else:
                tag_bin.add_record(entry)
        if count > 0:
            #query ES to get previous entries with the same tags from the last 4 hours
            print "Query ES for tag"
            res = es.search(\
                index="jag_hc2_documents", \
                doc_type="post", \
                body={
                    "query": {
                        "match":{
                            "tags": tag
                        }
                    },
                    "filter": {
                        "bool":{
                            "must" :[
                                {
                                    "range": {
                                        "post_date":{
                                            "gte" : "2014-10-01",
                                            "lte" : "2014-10-31"
                                         }
                                    }
                                }
                            ]
                        }
                    }
                }\
            )
            for hit in res["hits"]["hits"]:
                sr = ScoreRecord(hit, data_type=1)
                tag_bin.add_record(sr)

        #perform clustering on larger list
        tag_bin.cluster_and_write_to_es(0.001, 5)
