from fastapi import FastAPI
import app.worker as worker
from starlette.requests import Request
from starlette.responses import Response
import app.config as config
import gzip

fastApi = FastAPI()

@fastApi.get("/ping", status_code=201)
def ping():
    return 'pong!'

@fastApi.post("/v1/statement")
async def query(request: Request):
    user = request.headers.get('X-Presto-User')
    if not user:
        return config.getErrorMessage("xxxx", 'User name must be specified')

    body = await request.body()
    sql: str = bytes.decode(body)

    return worker.addPrestoJob(user, sql)

@fastApi.get("/v1/statement/{state}/{queryId}/{token}/{page}")
async def status(state : str, queryId : str, token : str, page : str):
    state = config.results.hget(queryId, config.STATE)
    if not state:
        return config.getErrorMessage(queryId, 'Unknown Query ID : ' + queryId)

    ## Wait till work is complete to provide results to facilitate Retry incase of errors.
    if state == config.STATE_DONE.encode():
        data = gzip.decompress(config.results.hget(queryId, page)).decode()
        return Response(content=data, media_type="application/json")

    ## Stream results as they become available.
    # data = config.rclient.hget(queryId, page)
    # if data:
    #     data = gzip.decompress(data).decode()
    #     return Response(content=data, media_type="application/json")

    return config.getExecutingMessage(queryId, page)
