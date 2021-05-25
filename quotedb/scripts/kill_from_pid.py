import os
import psutil
import sys


def killFromPid(pidfile):
    """KIll the process represented by the pid in pidfile if it is
    running and is a python process"""
    msg = ''
    if os.path.isfile(pidfile):
        with open(pidfile, 'r') as f:
            pid = f.read()
        if psutil.pid_exists(int(pid)):
            proc = psutil.Process(int(pid))
            if proc.status() == 'running' and proc.name().find('python') >= 0:
                print(f'killing {pid}')
                proc.kill()
                os.remove(pidfile)
                return
            else:
                msg += ":choosing not to kill process"
        else:
            msg += ":pid does not exist"
    else:
        msg += ":pidfile does not exit"
    print(msg)


if __name__ == '__main__':
    fn = 'c:/python/E/uw/quotedb/run/startcandles.pid'
    killFromPid(fn)
