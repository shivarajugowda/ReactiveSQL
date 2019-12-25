import subprocess
import pathlib

from app.worker import celeryApp

if __name__ == '__main__':

    pathlib.Path('logs').mkdir(exist_ok=True)

    # Start Webapp
    cmd = ['uvicorn', 'app.web:app', '--log-level', 'error', '--workers', '8']
    with open("logs/reactiveSQL.log", "wb") as out:
        subprocess.Popen(cmd, stdout=out, stderr=out)

    # Start Flower, Celery monitoring tool
    cmd = ['flower', '-A',  'app.worker', '--port=5555',  '--logfile', 'logs/flower.log']
    with open("logs/flower.log", "wb") as out:
        subprocess.Popen(cmd, stdout=out, stderr=out)

    celeryApp.start(argv=['celery', 'worker', '-l', 'info', '--logfile', 'logs/prestoWorker.log'])
