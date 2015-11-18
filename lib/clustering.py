import json
import pytz
from datetime import datetime, timedelta

def utc_to_local(utc_dt):
    # get integer timestamp to avoid precision lost
    local_tz = pytz.timezone('US/Eastern')
    local_dt = local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    assert utc_dt.resolution >= timedelta(microseconds=1)
    return local_dt.replace(microsecond=utc_dt.microsecond)

class ScoreRecord:
    def __init__(self, json_data):
        record = json.loads(json_data)
        self.lat = record["geo"]["coordinates"][0]
        self.lon = record["geo"]["coordinates"][1]
        self.text = record["text"]
        self.username = ["user"]["screen_name"]
        self.dt = utc_to_local(datetime.strptime(record["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
        self.img = record.img
        self.cluster = -1

    def toDict(self):
        obj = {
            'date': str(self.dt.date()),
            'datetime':str(self.dt),
            'cap': self.text,
            'usr': self.username,
            'lon': self.lon,
            'lat': self.lat,
            'cluster': self.cluster
        }
        if self.img is not None and len(self.img) > 0:
            obj['img'] = self.img
        return obj

class ScoreBin:
    def __init__(self, record=None):
        self.users = set([])
        self.lat = ''
        self.lon = ''
        self.dt = None
        self.records = []
        self.poly = []
        self.objPoly = None
        self.postsInHull = -1
        if record is not None:
            self.lat = record.lat
            self.lon = record.lon
            self.dt = record.dt
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
            'nTotal': len(self.records),
            'poly': list(self.poly),
            'background': self.postsInHull
            #'posts': map(lambda x: x.toDict(), self.records)
        }