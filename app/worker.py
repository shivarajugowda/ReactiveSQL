import sys, json, gzip, celery, subprocess, time, urllib3

import app.config as config
from urllib.parse import urlparse
from celery.signals import worker_init, worker_shutdown
from celery.task import periodic_task
from datetime import timedelta
from urllib3.util.retry import Retry
from celery.signals import celeryd_after_setup

celeryApp = celery.Celery('tasks', broker=config.CELERY_BROKER_URL, backend=config.CELERY_RESULT_BACKEND)
celeryApp.conf.update({
        'broker_pool_limit': 6,
        'redis_max_connections': 6,
        'worker_prefetch_multiplier': 1,
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
            config.results.hset(config.POD_PRESTO_SVC_MAP, config.POD_NAME, config.PRESTO_SVC);
            cmd = ['helm', 'install', config.PRESTO_SVC, './charts/presto', '--set', 'server.workers=0', '--wait']
            subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()

        url = 'http://' + config.PRESTO_SVC + ':' + str(config.PRESTO_PORT) + "/v1/cluster"
        print("Checking Presto Service with retry (timeout=100 seconds) : " + url)
        prestoHTTP.request('GET', url, retries=Retry(10, backoff_factor=0.1))  #-- Max wait on retry = 100 seconds
    except Exception as e:
        print("Failed to connect to Presto Service in " + str(time.perf_counter() - start)
                        + " seconds with exception : " + str(e))
        shutdown_presto_service()
        sys.exit(1)
    finally:
        print("Presto Service initialization Time Taken : " + str(time.perf_counter() - start))

@celeryd_after_setup.connect
def add_dynamic_queue(sender, instance, **kwargs):
    for n in range(10):
        instance.app.amqp.queues.select_add(config.QUEUE_PREFIX + str(n))

@worker_shutdown.connect()
def shutdown_presto_service(conf=None, **kwargs):
    if config.MANAGE_PRESTO_SERVICE:
        cmd = ['helm', 'uninstall', config.PRESTO_SVC]
        subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()
        config.results.hdel(config.POD_PRESTO_SVC_MAP, config.POD_NAME);


@periodic_task(run_every=timedelta(seconds=300), expires=15, ignore_result=True)
def garbageCollector():
    # TODO :
    print("If there are any zombie presto clusters, uninstall them:")

@celeryApp.task(compression='gzip', ignore_result=True, acks_late=True, reject_on_worker_lost=True,
                autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 10})
def runPrestoQuery(taskspec):
    task_id = runPrestoQuery.request.id
    url = 'http://' + config.PRESTO_SVC + ':' + str(config.PRESTO_PORT) + '/v1/statement'
    req = prestoHTTP.request('POST', url, body=taskspec['sql'], headers=taskspec['headers'])
    json_response = json.loads(req.data.decode())
    nexturi = json_response.get('nextUri', None)
    page = storeResults(task_id, 0, req.headers, json_response)
    while nexturi:
        req = prestoHTTP.request('GET', nexturi)
        json_response = json.loads(req.data.decode())
        nexturi = json_response.get('nextUri', None)
        page = storeResults(task_id, page, req.headers, json_response)
    config.results.hset(task_id, config.STATE, config.STATE_DONE)

def storeResults(task_id: str, page, headers, json_response):
    # Based on thee tests so far, I think we can ignore the QUEUED responses.
    if 'QUEUED' == json_response['stats']['state']:
        return page

    # Check for retryable errors.
    if 'error' in json_response and 'message' in json_response['error']:
        errmsg = json_response['error']['message']
        if any(pattern in errmsg for pattern in config.RETRYABLE_ERROR_MESSAGES):
            raise RuntimeError(errmsg) # Raise error, the query will be retried.

    if 'nextUri' in json_response : # Switch the URI to point to the stored results.  # Tested on Presto 317
        parsed = urlparse(json_response['nextUri'])
        parsed = parsed._replace(netloc=config.WEB_SERVICE, path=parsed.path.replace(json_response['id'], task_id))
        json_response['nextUri'] = parsed.geturl()

    json_response['id'] = task_id

    # Write to Redis.
    config.results.hset(task_id , page, gzip.compress(json.dumps(json_response).encode()))
    prestoHeaders = {key: val for key, val in headers.items() if key.startswith("X-Presto")}
    if prestoHeaders:
        config.results.hset(task_id, str(page) + "_headers", json.dumps(prestoHeaders))

    return page + 1


if __name__ == '__main__':
    runPrestoQuery("Select 1")
