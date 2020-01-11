import os, redis

from pydantic import BaseModel
import uuid, socket

CELERY_BROKER_URL     = os.environ.get('CELERY_BROKER_URL')  if os.environ.get('CELERY_BROKER_URL')  else 'amqp://guest:guest@localhost:5672'
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND') if os.environ.get('CELERY_RESULT_BACKEND') else  "redis://localhost:6379/0"
GATEWAY_SERVICE       = os.environ.get('GATEWAY_SERVICE')  if  os.environ.get('GATEWAY_SERVICE') else "localhost:8000"
MANAGE_PRESTO_SERVICE = os.environ.get('MANAGE_PRESTO_SERVICE')

PRESTO_SVC = 'presto-' + str(uuid.uuid4())[:12] if MANAGE_PRESTO_SERVICE else "localhost"
PRESTO_PORT = 8080

RESULTS_TIME_TO_LIVE_SECS = 600
QUEUE_PREFIX = "prestoworker:"
POD_PRESTO_SVC_MAP = "pod-presto-map:"

POD_NAME = socket.gethostname()
results = redis.from_url(CELERY_RESULT_BACKEND)

RETRYABLE_ERROR_MESSAGES = ['Presto server is still initializing',      # Presto coordinator is restarting.
                            ]
STATE = "state"
STATE_QUEUED = "QUEUED"
STATE_RUNNING = "RUNNING"
STATE_DONE = "DONE"

class Stats(BaseModel):
    state : str = 'QUEUED'
    queued : bool = True
    scheduled : bool = False
    nodes : int = 0
    totalSplits : int = 0
    queuedSplits: int = 0
    runningSplits: int = 0
    completedSplits: int = 0
    cpuTimeMillis: int = 0
    wallTimeMillis: int = 0
    queuedTimeMillis : int = 0
    elapsedTimeMillis : int = 0
    processedRows : int = 0
    processedBytes : int = 0
    peakMemoryBytes : int = 0
    spilledBytes: int = 0

class QueryResult(BaseModel):
    id : str
    infoUri : str = None
    nextUri : str = None
    stats : Stats = Stats()
    error: str = None
    warnings = []

def getQueuedMessage(taskId: str):
    nextUri = 'http://' + GATEWAY_SERVICE + '/v1/statement/queued/' + taskId + '/zzz/0'
    infoUri = 'http://' + GATEWAY_SERVICE + '/ui/query.html?' + taskId
    return QueryResult(id=taskId, nextUri=nextUri, infoUri=infoUri)

def getExecutingMessage(taskId: str, page: int):
    nextUri = 'http://' + GATEWAY_SERVICE + '/v1/statement/executing/' + taskId + '/zzz/' + page
    infoUri = 'http://' + GATEWAY_SERVICE + '/ui/query.html?' + taskId
    qr = QueryResult(id=taskId, nextUri=nextUri, infoUri=infoUri);
    qr.stats.state = "RUNNING"; qr.stats.queued = False; qr.stats.scheduled = True
    return qr;

def getErrorMessage(taskId: str, error: str):
    nextUri = 'http://' + GATEWAY_SERVICE + '/v1/statement/queued/' + taskId + '/zzz/0'
    return QueryResult(id=taskId, nextUri=nextUri, error=error)