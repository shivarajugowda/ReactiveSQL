
from urllib.parse import ParseResult, urlsplit
import os, celery, json, redis, gzip, copy, http.client


CELERY_BROKER = os.environ.get('CELERY_BROKER')
CELERY_BACKEND = os.environ.get('CELERY_BACKEND')
#CELERY_BROKER = "redis://localhost:6379/0"
#CELERY_BACKEND = "redis://localhost:6379/0"


celeryApp = celery.Celery('tasks', broker=CELERY_BROKER, backend=CELERY_BACKEND)
#celeryApp.conf.update({
#    'broker_pool_limit': '30',
#    })

rclient = redis.from_url(CELERY_BROKER)
connection = http.client.HTTPConnection(host='localhost', port=9080)

# Requests is slow. Here is the benchmark.
# Benchmark 1000 'SELECT 1' queries run sequentially and time per query in ms
#       BASELINE going direct to Presto : 12 ms
#       REQUESTS library : 64 ms
#       http.client : 21 ms.
@celeryApp.task
def runQuery(sql: str):
    page = 0
    task_id = runQuery.request.id

    rclient.expire(task_id, 300)
    connection.request('POST', '/v1/statement', body=sql, headers={'X-Presto-User': 'XYZ'})
    json_response = json.loads(connection.getresponse().read().decode())
    page = storeResults(task_id, copy.copy(json_response), page)
    while('nextUri' in json_response):
        connection.request('GET', json_response['nextUri'])
        json_response = json.loads(connection.getresponse().read().decode())
        page = storeResults(task_id, copy.copy(json_response), page)

    rclient.hset(task_id, "DONE", 1)

def storeResults(task_id : str, json_response, page):
    # We can ignore the QUEUED results. I think...
    if 'QUEUED' == json_response['stats']['state']:
        return page

    # Switch the URI to point to the stored results.
    if 'nextUri' in json_response:
        urix = urlsplit(json_response['nextUri'])
        parts = urix.path.split("/")
        parts[4] = task_id; parts[5] = 'zzz'
        res = ParseResult(scheme=urix.scheme, netloc='localhost:8000', path='/'.join(parts),
                          params=None, query=None, fragment=None)
        json_response['nextUri'] = res.geturl()

    json_response['id'] = task_id
    rclient.hset(task_id, page, gzip.compress(json.dumps(json_response).encode()))
    return page + 1


if __name__ == '__main__':
    runQuery("Select 1")