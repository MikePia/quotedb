import logging
import psutil


def panicKillCandles(modulename, pname='python'):
    '''
    Explanation
    -----------
    Use this only if a better method is not available from a running program.
    Created to kill the startCandles call but can be used more generally to kill a running
    python program in a different process.

    Parameters
    ----------
    :params: modulname: str: The module name of the process to be killed. For example if
    it was called like python quotedb/mytest4.py, then 'mytest4.py' should be specific enough
    :params: pname: str: process name.
    '''
    for proc in psutil.process_iter():
        # check whether the process name matches
        try:
            cmd = proc.cmdline()
        except Exception:
            continue

        if len(cmd) > 1 and cmd[-1].find(modulename) >= 0:
            try:
                pn = proc.name()
                if pn.find(pname) >= 0:
                    pid = proc.ppid()
                    print('killing process', pid)
                    proc.kill()
                    return pid
            except Exception:
                continue

    logging.error(f"Process not found for module {modulename}")
    return -1


if __name__ == '__main__':
    # Kill the process started by the module 'startcandles.py'
    panicKillCandles('getdata.py')
