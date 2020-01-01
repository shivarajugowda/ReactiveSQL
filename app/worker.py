import sys, json, gzip, copy, celery, subprocess, time, urllib3
import app.config as config
from urllib.parse import ParseResult, urlsplit
from celery.signals import worker_init, worker_shutting_down
from celery.task import periodic_task
from datetime import timedelta
from urllib3.util.retry import Retry

celeryApp = celery.Celery('tasks', broker=config.CELERY_BROKER_URL, backend=config.CELERY_RESULT_BACKEND)
celeryApp.conf.update({
        'broker_pool_limit': 6,
        'redis_max_connections': 6,
        'worker_prefetch_multiplier': 1,
        'task_acks_late': True,
     })

prestoHTTP = urllib3.PoolManager(retries=Retry(5, backoff_factor=0.1)) #-- Max wait on retry = 3.0 seconds


## Start and Check Presto Service.
# TODO: The print statements in this function are lost in the container env. because the function is run in a subprocess,
#       need to figure out a way to persist it in the logs.
@worker_init.connect()
def start_presto_service(conf=None, **kwargs):
    start = time.perf_counter()
    try:
        if config.MANAGE_PRESTO_SERVICE:
            config.rclient.hset(config.POD_PRESTO_SVC_MAP, config.POD_NAME, config.PRESTO_SVC);
            cmd = ['helm', 'install', config.PRESTO_SVC, './charts/presto', '--set', 'server.workers=0', '--wait']
            subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()

        url = 'http://' + config.PRESTO_SVC + ':' + str(config.PRESTO_PORT)
        print("Checking Presto Service with retry (timeout=100 seconds) : " + url)
        prestoHTTP.request('GET', url, retries=Retry(10, backoff_factor=0.1))  #-- Max wait on retry = 100 seconds
    except Exception as e:
        print("Failed to connect to Presto Service in " + str(time.perf_counter() - start)
                        + " seconds with exception : " + str(e))
        if config.MANAGE_PRESTO_SERVICE:
            cmd = ['helm', 'uninstall', config.PRESTO_SVC]
            subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()
        sys.exit(1)
    finally:
        print("Presto Service initialization Time Taken : " + str(time.perf_counter() - start))

@worker_init.connect()
def subscribeToActiveQueues(conf=None, **kwargs):
    for key in config.rclient.scan_iter(config.QUEUE_PREFIX + "*"):
        celeryApp.control.add_consumer(key.decode())

@worker_shutting_down.connect()
def shutdown_presto_service(conf=None, **kwargs):
    if config.MANAGE_PRESTO_SERVICE:
        cmd = ['helm', 'uninstall', config.PRESTO_SVC]
        subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()
        config.rclient.hdel(config.POD_PRESTO_SVC_MAP, config.POD_NAME);
        print("Stop Presto Service ")

@periodic_task(run_every=timedelta(seconds=300), expires=15, ignore_result=True)
def garbageCollector():
    print("If there are any zombie presto clusters, uninstall them:")

@celeryApp.task(compression='gzip', ignore_result=True, acks_late=True)
def runPrestoQuery(sql: str):
    task_id = runPrestoQuery.request.id
    url = 'http://' +  config.PRESTO_SVC + ':' + str(config.PRESTO_PORT) + '/v1/statement'
    req = prestoHTTP.request('POST', url, body=sql, headers={'X-Presto-User': 'XYZ'})
    json_response = json.loads(req.data.decode())
    page = storeResults(task_id, copy.copy(json_response), 0)
    while('nextUri' in json_response):
        req = prestoHTTP.request('GET', json_response['nextUri'])
        json_response = json.loads(req.data.decode())
        page = storeResults(task_id, copy.copy(json_response), page)
    config.rclient.hset(task_id, config.STATE, config.STATE_DONE)

def storeResults(task_id: str, json_response, page):
    # We can ignore the QUEUED results. I think...
    if 'QUEUED' == json_response['stats']['state']:
        return page

    # Switch the URI to point to the stored results.  # Tested on Presto 317
    if 'nextUri' in json_response:
        urix = urlsplit(json_response['nextUri'])
        parts = urix.path.split("/")
        parts[4] = task_id;
        parts[5] = 'zzz'

        res = ParseResult(scheme=urix.scheme, netloc=config.WEB_SERVICE, path='/'.join(parts),
                          params=None, query=None, fragment=None)
        json_response['nextUri'] = res.geturl()

    json_response['id'] = task_id
    config.rclient.hset(task_id , page, gzip.compress(json.dumps(json_response).encode()))
    return page + 1

def addPrestoJob(user: str, sql: str):
    queueName = config.QUEUE_PREFIX + user
    len = config.rclient.llen(queueName)
    if len > 10:
        return config.getErrorMessage("xxxx", 'Max concurrent queries reached')

    task = runPrestoQuery.apply_async((sql,), queue=queueName)

    # Create result stub and set TTL(time to live) for the query result.
    if not config.rclient.hexists(task.id, config.STATE):
        config.rclient.hset(task.id, config.STATE, config.STATE_PENDING)
        config.rclient.expire(task.id, config.RESULTS_TIME_TO_LIVE_SECS)

    ## If the queue is newly created, tell workers to start consuming from it.
    if len == 0:
        celeryApp.control.add_consumer(queueName)

    return config.getQueuedMessage(task.id)


if __name__ == '__main__':
    runPrestoQuery("Select 1")
