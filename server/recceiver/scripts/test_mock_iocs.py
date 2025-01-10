import os
import threading
import sys
import time
import signal

iocexecutable = "st.cmd"


def startIOC():
    # conf needs to be set
    pid, fd = os.forkpty()
    if pid == 0:
        os.chdir("../../../client/iocBoot/iocdemo")
        print(os.curdir)
        os.execv("st_test.cmd", [""])
    return pid, fd


def readfd(fd):
    while 1:
        _ = str(os.read(fd, 16384).strip("\n"))


def handler(signum, frame):
    global pids
    for pid in pids:
        os.kill(pid, signal.SIGKILL)
    sys.exit()


def main():
    global pids
    pids = []
    signal.signal(signal.SIGTERM, handler)
    os.chdir(
        os.path.dirname(os.path.abspath(sys.argv[0]))
    )  # Uses a filename, not good, also only works on linux?
    threads = []
    for i in range(1, 100):
        iocpid, iocfd = startIOC()
        pids.append(iocpid)
        print("len pids: ", len(pids))
        iocthread = threading.Thread(
            group=None, target=readfd, args=(iocfd,), name="iocthread", kwargs={}
        )
        threads.append(iocthread)
        iocthread.start()
    try:
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        sys.exit()


if __name__ == "__main__":
    main()
