"""Run scripts from this directory
The scripts in this directory will create a pid file in {RUNDIR}/{filename}.pid
and can be killed using killFromPid(pidfile)
"""
import os
import sys
import subprocess
import time

# using command mkdir
# a = 'python'


def runScript(fn, kwargs=None):
    dirname, _ = os.path.split(__file__)
    fn = os.path.join(dirname, fn)
    if not os.path.exists(fn):
        print(f'file not found {os}')
        return
    
    args = [sys.executable, fn]
    for key in kwargs.keys():
        args.append(key)
        args.append(kwargs[key])

    # args = ' '.join(args)

    # code = subprocess.Popen(arg, shell=True)
    # code = subprocess.Popen(args, shell=True, start_new_session=True, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    my_env = os.environ
    code = subprocess.Popen(args, env=my_env)
    print(f'\n============================================== {code} =====================================')
    # subprocess.Popen(arg, shell=True, stdout=None, stdin=None, stderr=None)
    # print(f'code is {code}')


if __name__ == '__main__':
    """
    Serves as a test. The python continues after starting the script. But if
    this script were to end, the process would also end.
    """
    from quotedb.scripts.kill_from_pid import  killFromPid
    fn = 'startcandles.py'
    kwargs = {"-s": "nasdaq100", "-m": "allquotes", "-d": "5-17-2021 9:30"}
    # -n = 0, -l = True
    if not os.environ.get('RUNDIR'):
        from dotenv import load_dotenv
        directory = os.path.normpath(__file__ + "..")
        load_dotenv(dotenv_path=directory)
    runScript(fn, kwargs)
    pidfile = os.path.join(os.environ['RUNDIR'], "startcandles.pid")

    while True:
        time.sleep(50)
        killFromPid(pidfile)
