from fastapi import FastAPI, Header
import app.worker as worker
from starlette.requests import Request
from starlette.responses import Response
import app.config as config
import gzip, json

fastApi = FastAPI()

@fastApi.get("/ping", status_code=201)
def ping():
    return 'pong!'

@fastApi.post("/v1/statement")
async def query(request: Request, x_presto_user: str = Header(None), x_presto_prepared_statement: str = Header(None)):
    body = await request.body()
    sql: str = bytes.decode(body)

    taskspec = {}
    taskspec['headers'] = {key: val for key, val in request.headers.items() if key.startswith("x-presto")}
    taskspec['sql'] = sql

    # TODO : Map user to the accountid queue.
    queueName = config.QUEUE_PREFIX + "5"

    # RATE LIMIT
    # len = config.broker.llen(queueName)
    # if len > 10:
    #     return config.getErrorMessage("xxxx", 'Max concurrent queries reached')

    task = worker.runPrestoQuery.apply_async((taskspec,), queue=queueName)

    # Create result stub and set TTL(time to live) for the query result.
    if not config.results.hexists(task.id, config.STATE):
        config.results.hset(task.id, config.STATE, config.STATE_PENDING)
        config.results.expire(task.id, config.RESULTS_TIME_TO_LIVE_SECS)

    return config.getQueuedMessage(task.id)

@fastApi.get("/v1/statement/{state}/{queryId}/{token}/{page}")
async def status(state : str, queryId : str, token : str, page : str):
    state = config.results.hget(queryId, config.STATE)
    if not state:
        return config.getErrorMessage(queryId, 'Unknown Query ID : ' + queryId)

    ## Wait till work is complete to provide results to facilitate Retry incase of errors.
    if state == config.STATE_DONE.encode():
        headers = config.results.hget(queryId, page + "_headers")
        if headers:
            headers = json.loads(headers.decode())
        data = gzip.decompress(config.results.hget(queryId, page)).decode()
        return Response(headers=headers, content=data, media_type="application/json")

    return config.getExecutingMessage(queryId, page)
