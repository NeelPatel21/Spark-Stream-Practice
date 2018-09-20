import datetime

from pyspark.streaming import DStream


def logParser(row):
    r = row.split(',')
    dt = datetime.datetime.strptime(r[0],'%Y-%m-%d %H:%M:%S.%f')
    ip = r[1]
    vrb = r[2]
    uri = r[3]
    resp = int(r[4])
    return dt, ip, vrb, uri, resp


def requestTypeCounter(ds: DStream) -> DStream:
    nds = ds.map(lambda x: (x[2], x)).combineByKey(lambda x: 1, lambda x,y: x+2, lambda x,y: x+y)
    return nds


def responseTypeCounter(ds: DStream) -> DStream:
    nds = ds.map(lambda x: (x[4], x)).combineByKey(lambda x: 1, lambda x,y: x+2, lambda x,y: x+y)
    return nds

