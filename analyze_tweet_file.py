import os
import sys
sys.path.insert(0, './lib/')
from clustering import ScoreRecord, ScoreBin

def analyze_recent(tweet_file_path):
    files = sorted(os.listdir(tweet_file_path), key=lambda x: os.stat(os.path.join(tweet_file_path, x)).st_mtime)
    dict_all_hastags = {}
    for file in files:
        d0 = open(file)
        for line in d0:
            sr = ScoreRecord(line)