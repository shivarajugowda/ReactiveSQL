import orjson, gzip, copy, http.client, celery, subprocess, time
import app.config as config
from urllib.parse import ParseResult, urlsplit
from celery.signals import worker_init, worker_shutting_down
from celery.task import periodic_task
import kubernetes
from datetime import timedelta

celeryApp = celery.Celery('tasks', broker=config.CELERY_BROKER, backend=config.CELERY_BACKEND)
celeryApp.conf.update({
#     'task_serializer' : 'msgpack',
        'broker_pool_limit': 6,
        'redis_max_connections': 6,
        'worker_prefetch_multiplier': 1,
        'task_acks_late': True,
        'celery_ignore_result': True
     })

@worker_init.connect()
def configure_worker_init(conf=None, **kwargs):
    # cmd = ['helm', 'install', 'mypresto', 'stable/presto', '--set', 'server.workers=0', '--set', 'server.jvm.maxHeapSize=3G', '--wait']
    # process = subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()
    # print("Helm returned with ", process.returncode)

    # Consume from active queues.
    for key in config.rclient.scan_iter(config.QUEUE_PREFIX + "*"):
        celeryApp.control.add_consumer(key)

@worker_shutting_down.connect()
def configure_worker_shutdown(conf=None, **kwargs):
    print("SHIV : Worker SHUTTING DOWN")
    # cmd = ['helm', 'uninstall', 'mypresto']
    # process = subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()

@periodic_task(run_every=timedelta(seconds=120), expires=15, ignore_result=True)
def autoscale():
    kubernetes.config.load_kube_config()
    v1 = kubernetes.client.CoreV1Api()
    print("Listing pods with their IPs:")
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for i in ret.items:
        print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

@celeryApp.task(compression='gzip', ignore_result=True, acks_late=True)
def runPrestoQuery(sql: str):
    page = 0
    task_id = runPrestoQuery.request.id

    start = time.perf_counter()
    connection = http.client.HTTPConnection(host='localhost', port=9080)

    connection.request('POST', '/v1/statement', body=sql, headers={'X-Presto-User': 'XYZ'})
    json_response = orjson.loads(connection.getresponse().read())
    page = storeResults(task_id, copy.copy(json_response), page)
    while('nextUri' in json_response):
        connection.request('GET', json_response['nextUri'])
        json_response = orjson.loads(connection.getresponse().read())
        page = storeResults(task_id, copy.copy(json_response), page)

    config.rclient.hset(task_id, "DONE", 1)
    connection.close()
    #print("Time taken : " + str(time.perf_counter() - start))

def storeResults(task_id: str, json_response, page):
    # We can ignore the QUEUED results. I think...
    if 'QUEUED' == json_response['stats']['state']:
        return page

    # Switch the URI to point to the stored results.
    if 'nextUri' in json_response:
        urix = urlsplit(json_response['nextUri'])
        parts = urix.path.split("/")
        parts[4] = task_id;
        parts[5] = 'zzz'
        res = ParseResult(scheme=urix.scheme, netloc=config.WEB_SERVICE, path='/'.join(parts),
                          params=None, query=None, fragment=None)
        json_response['nextUri'] = res.geturl()

    json_response['id'] = task_id
    config.rclient.hset(task_id , page, gzip.compress(orjson.dumps(json_response)))
    # TTL : Set time to live for the query result.
    if page == 0:
        config.rclient.expire(task_id, config.RESULTS_TIME_TO_LIVE_SECS)
    return page + 1

def addPrestoJob(user: str, sql: str):
    queueName = config.QUEUE_PREFIX + user
    len = config.rclient.llen(queueName)
    if len > 10:
        raise AssertionError('Max concurrent queries reached')

    task = runPrestoQuery.apply_async((sql,), queue=queueName)

    ## If the queue is newly created, tell workers to start consuming from it.
    if len == 0:
        celeryApp.control.add_consumer(queueName)

    return task.id

if __name__ == '__main__':
    runPrestoQuery("Select 1")