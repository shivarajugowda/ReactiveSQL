import subprocess
import pathlib

# This file is only used for debugging on the local env and is not used in K8S deployment.


from app.worker import celeryApp
if __name__ == '__main__':

    pathlib.Path('logs').mkdir(exist_ok=True)

    # Start Webapp
    cmd = ['uvicorn', 'app.web:fastApi', '--log-level', 'info', '--workers', '4']
    with open("logs/web.log", "wb") as out:
        subprocess.Popen(cmd, stdout=out, stderr=out)

    # Start Flower, Celery UI
    cmd = ['flower', '-A',  'app.worker', '--port=5555',  '--logfile', 'logs/flower.log']
    with open("logs/flower.log", "wb") as out:
        subprocess.Popen(cmd, stdout=out, stderr=out)

    celeryApp.start(argv=['celery', 'worker', '-O', 'fair', '--beat', '-S', 'redbeat.RedBeatScheduler', '-l', 'info',  '--logfile', 'logs/worker.log'])
    #celeryApp.start(argv=['celery', 'worker', '-O', 'fair', '-P', 'eventlet',   '-l', 'error', '--logfile', 'logs/worker.log'])