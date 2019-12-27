import os, redis
from pydantic import BaseModel

CELERY_BROKER = os.environ.get('CELERY_BROKER')     #  "redis://localhost:6379/0"
CELERY_BACKEND = os.environ.get('CELERY_BACKEND')   #  "redis://localhost:6379/0"
WEB_SERVICE = os.environ.get('WEB_SERVICE')      #  "myrx-reactivesql-web:8000"
RESULTS_TIME_TO_LIVE_SECS = 300
QUEUE_PREFIX = "prestoworker:"

rclient = redis.from_url(CELERY_BROKER)

class Stats(BaseModel):
    state : str = 'QUEUED'
    queued : bool = True
    scheduled : bool = False
    nodes : int = 0
    totalSplits : int = 0

class QueryResult(BaseModel):
    id : str
    infoUri : str = None
    nextUri : str = None
    stats : Stats = Stats()

def getQueuedStatus(taskId: str):
    nextUri = 'http://' + WEB_SERVICE + '/v1/statement/queued/' + taskId + '/zzz/0'
    infoUri = 'http://' + WEB_SERVICE + '/ui/query.html?' + taskId
    return QueryResult(id=taskId, nextUri=nextUri, infoUri=infoUri)

def getExecutingStatus(taskId: str, page: int):
    nextUri = 'http://' + WEB_SERVICE + '/v1/statement/executing/' + taskId + '/zzz/' + page
    infoUri = 'http://' + WEB_SERVICE + '/ui/query.html?' + taskId
    qr = QueryResult(id=taskId, nextUri=nextUri, infoUri=infoUri);
    qr.stats.state = "RUNNING"; qr.stats.queued = False; qr.stats.scheduled = True
    return qr;