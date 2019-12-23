from pydantic import BaseModel


class Stats(BaseModel):
    state : str = 'QUEUED'
    queued : bool = True
    scheduled : bool = False
    nodes : int = 0
    totalSplits : int = 0

class QueryResult(BaseModel):
    id : str
    infoUri : str = None
    nextUri : str = None
    stats : Stats = Stats()

def getQueuedStatus(taskId: str):
    nextUri = 'http://localhost:8000/v1/statement/queued/' + taskId + '/zzz/0'
    infoUri = 'http://localhost:8000/ui/query.html?' + taskId
    return QueryResult(id=taskId, nextUri=nextUri, infoUri=infoUri)

