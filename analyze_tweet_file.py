import os
import sys
import elasticsearch
sys.path.insert(0, './lib/')
from clustering import ScoreRecord, ScoreBin

def analyze_recent(tweet_file_path, es_url=None):
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
        break

    for tag, lst_rec in new_records.keys():
        print "Getting data for tag: ", tag
        count = es.count(index="jag_hc2_documents", doc_type="post", q="tags:"+tag)["count"]
        #test if there is enough entries for clustering
        if (count+len(lst_rec))<5:
            for sr in lst_rec:
                sr.write_to_es("jag_hc2_documents","post",es)
            continue
        #query ES to get previous entries with the same tags from the last 8 hours
        #associate querries with the existing hashtag list
        #perform clustering on larger list
        #execute steps on case based list

