from fastapi import FastAPI
import app.worker as worker
from starlette.requests import Request
from starlette.responses import Response
import app.config as config
import gzip

fastApi = FastAPI()

@fastApi.get("/ping")
async def ping():
    return 'pong!'

@fastApi.post("/v1/statement")
async def query(request: Request):
    body = await request.body()
    sql: str = bytes.decode(body)
    user = request.headers.get('X-Presto-User')

    if not user:
        raise AssertionError('User name must be specified')

    taskId = worker.runPrestoJob(user, sql)
    return config.getQueuedStatus(taskId)

@fastApi.get("/v1/statement/{state}/{queryId}/{token}/{page}")
async def status(state : str, queryId : str, token : str, page : str):
    ## Block execution till work is complete. Retry becomes easy.
    if config.rclient.hexists(queryId, "DONE") :
        data = gzip.decompress(config.rclient.hget(queryId, page)).decode()
        return Response(content=data, media_type="application/json")

    ## Stream results as they become available.
    # data = config.rclient.hget(queryId, page)
    # if data:
    #     data = gzip.decompress(data).decode()
    #     return Response(content=data, media_type="application/json")

    return config.getExecutingStatus(queryId,page)
