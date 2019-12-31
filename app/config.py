import os, redis
from pydantic import BaseModel
import uuid, socket

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL')     #  "redis://localhost:6379/0"
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND')   #  "redis://localhost:6379/0"
WEB_SERVICE = os.environ.get('WEB_SERVICE')      #  "locahost:8000"
RESULTS_TIME_TO_LIVE_SECS = 300
QUEUE_PREFIX = "prestoworker:"
POD_NAME = socket.gethostname()

# CELERY related env variables.
CELERYD_TASK_SOFT_TIME_LIMIT = 60 * 60 # None of our tasks should take more than 1 hour
CELERYD_TASK_TIME_LIMIT = CELERYD_TASK_SOFT_TIME_LIMIT + 10 # If soft limit didn't help, kill the task right after
CELERYD_MAX_TASKS_PER_CHILD = 500  # prevent memory leaks
CELERY_ACKS_LATE = True  # has to have idempotent tasks!
CELERY_TIMEZONE = 'UTC' # default, but be sure
CELERY_TASK_SERIALIZER = 'json' # restore to pickle in dire situations
CELERY_RESULT_SERIALIZER = 'json'
CELERY_IGNORE_RESULT = False  # performance

rclient = redis.from_url(CELERY_BROKER_URL)

STATE = "state"
STATE_PENDING = "PENDING"
STATE_RUNNING = "RUNNING"
STATE_DONE = "DONE"

MANAGE_PRESTO_SERVICE = os.environ.get('MANAGE_PRESTO_SERVICE')
POD_PRESTO_SVC_MAP = "pod-presto-map:"

if MANAGE_PRESTO_SERVICE:
    PRESTO_SVC = 'mypresto-' + str(uuid.uuid4())[:8]
    PRESTO_PORT = 8080
else :
    PRESTO_SVC = "localhost"
    PRESTO_PORT = 9080


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
    error:    str = None
    stats : Stats = Stats()

def getQueuedMessage(taskId: str):
    nextUri = 'http://' + WEB_SERVICE + '/v1/statement/queued/' + taskId + '/zzz/0'
    infoUri = 'http://' + WEB_SERVICE + '/ui/query.html?' + taskId
    return QueryResult(id=taskId, nextUri=nextUri, infoUri=infoUri)

def getExecutingMessage(taskId: str, page: int):
    nextUri = 'http://' + WEB_SERVICE + '/v1/statement/executing/' + taskId + '/zzz/' + page
    infoUri = 'http://' + WEB_SERVICE + '/ui/query.html?' + taskId
    qr = QueryResult(id=taskId, nextUri=nextUri, infoUri=infoUri);
    qr.stats.state = "RUNNING"; qr.stats.queued = False; qr.stats.scheduled = True
    return qr;

def getErrorMessage(taskId: str, error: str):
    nextUri = 'http://' + WEB_SERVICE + '/v1/statement/queued/' + taskId + '/zzz/0'
    return QueryResult(id=taskId, nextUri=nextUri, error=error)