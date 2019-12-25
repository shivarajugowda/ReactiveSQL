from fastapi import FastAPI
from app.worker import runQuery, rclient
from app.jsonData import getQueuedStatus
from starlette.requests import Request
from starlette.responses import Response

import gzip

app = FastAPI()

@app.get("/ping")
async def ping():
    return 'pong!'

@app.post("/v1/statement")
async def query(request: Request):
    body = await request.body()
    sql: str = bytes.decode(body)
    task = runQuery.delay(sql)
    return getQueuedStatus(task.id)

@app.get("/v1/statement/{state}/{queryId}/{token}/{page}")
async def status(state : str, queryId : str, token : str, page : str):
    if rclient.hget(queryId, "DONE") :
        data = gzip.decompress(rclient.hget(queryId, page)).decode()
        return Response(content=data, media_type="application/json")
    return getQueuedStatus(queryId)
