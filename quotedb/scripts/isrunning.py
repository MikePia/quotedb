import os
import psutil


def is_running(script):
    script = os.path.splitext(script)[0] + ".pid"
    pidfile = os.path.join(os.environ['RUNDIR'], script)
    if os.path.isfile(pidfile):
        with open(pidfile, 'r') as f:
            pid = f.read()
        pid = int(pid)
        if psutil.pid_exists(pid):
            proc = psutil.Process(int(pid))
            if proc.status() == "running":
                return True
        os.remove(pidfile)
    return False

    
if __name__ == '__main__':
    import argparse
    import dotenv
    dotenv.load_dotenv()

    p = argparse.ArgumentParser()
    p.add_argument("-s", "--script", type=str, required=True)
    args=p.parse_args()
    print(is_running(args.script))