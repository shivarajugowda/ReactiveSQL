import subprocess
import pathlib

from app.worker import celeryApp

if __name__ == '__main__':

    pathlib.Path('logs').mkdir(exist_ok=True)

    # Start Webapp
    cmd = ['uvicorn', 'app.web:fastApi', '--log-level', 'error', '--workers', '4']
    with open("logs/web.log", "wb") as out:
        subprocess.Popen(cmd, stdout=out, stderr=out)

    # Start Flower, Celery UI
    cmd = ['flower', '-A',  'app.worker', '--port=5555',  '--logfile', 'logs/flower.log']
    with open("logs/flower.log", "wb") as out:
        subprocess.Popen(cmd, stdout=out, stderr=out)

    #celeryApp.start(argv=['celery', 'worker', '-l', 'warning',  '-P', 'eventlet',  '--logfile', 'logs/worker.log'])
    celeryApp.start(argv=['celery', 'worker', '--beat', '-S', 'redbeat.RedBeatScheduler', '-l', 'warning',  '--logfile', 'logs/worker.log'])