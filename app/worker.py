import json, gzip, copy, celery, subprocess, time, urllib3, http.client
import app.config as config
from urllib.parse import ParseResult, urlsplit
from celery.signals import worker_init, worker_shutting_down
from celery.task import periodic_task
import kubernetes
from datetime import timedelta
from urllib3.util.retry import Retry

prestoHTTP = urllib3.PoolManager(retries=Retry(connect=5, read=5))
celeryApp = celery.Celery('tasks', broker=config.CELERY_BROKER_URL, backend=config.CELERY_RESULT_BACKEND)
celeryApp.conf.update({
        'broker_pool_limit': 6,
        'redis_max_connections': 6,
        'worker_prefetch_multiplier': 1,
        'task_acks_late': True,
     })

@worker_init.connect()
def configure_worker_init(conf=None, **kwargs):
    if config.MANAGE_PRESTO_SERVICE:
        start = time.perf_counter()
        config.rclient.hset(config.POD_PRESTO_SVC_MAP, config.POD_NAME, config.PRESTO_SVC);
        cmd = ['helm', 'install', config.PRESTO_SVC, './charts/presto', '--set', 'server.workers=0', '--wait']
        rtcode = subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()
        if rtcode!=0 :
            raise RuntimeError("Failed to start Presto Service ")
        print("Start Presto Service : time taken (secs) : " + str(time.perf_counter() - start))

    # Consume from active queues.
    for key in config.rclient.scan_iter(config.QUEUE_PREFIX + "*"):
        celeryApp.control.add_consumer(key)

@worker_shutting_down.connect()
def configure_worker_shutdown(conf=None, **kwargs):
    if config.MANAGE_PRESTO_SERVICE:
        cmd = ['helm', 'uninstall', config.PRESTO_SVC]
        rtcode = subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()
        config.rclient.hdel(config.POD_PRESTO_SVC_MAP, config.POD_NAME);
        print("Stop Presto Service ")

@periodic_task(run_every=timedelta(seconds=300), expires=15, ignore_result=True)
def autoscale():
    kubernetes.config.load_kube_config()
    v1 = kubernetes.client.CoreV1Api()
    print("Listing pods with their IPs:")
    ret = v1.list_namespaced_pod('default', watch=False)
    for i in ret.items:
        print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

@periodic_task(run_every=timedelta(seconds=300), expires=15, ignore_result=True)
def garbageCollector():
    print("Run garbage Collector:")


@celeryApp.task(compression='gzip', ignore_result=True, acks_late=True)
def runPrestoQuery(sql: str):
    task_id = runPrestoQuery.request.id
    req = prestoHTTP.request('POST', 'http://localhost:9080/v1/statement', body=sql, headers={'X-Presto-User': 'XYZ'})
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

    # Switch the URI to point to the stored results.
    if 'nextUri' in json_response:
        # Tested on Presto 317
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
