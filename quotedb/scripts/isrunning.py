import os
import psutil


def is_running(script):
    script = os.path.splitext(script)[0] + ".pid"
    pidfile = os.path.join(os.environ['RUNDIR'], script)
    if os.path.isfile(pidfile):
        with open(pidfile, 'r') as f:
            pid = f.read()
        proc = psutil.Process(int(pid))
        if proc.status() == "running":
            return True
    return False
