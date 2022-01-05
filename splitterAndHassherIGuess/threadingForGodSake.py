import somethongMoreComplicatedWithJson as sig
from queue import Queue
from os import listdir
from os.path import isfile, isdir, join
import threading
import sys

def main(myPath=None, outputPath=None, numberOfThreads=None):
    if myPath == None:
        if sys.argv[0] == sys.argv[-1]:
            print("No file specified!")
            sys.exit()
        else:
            myPath = sys.argv[-1]
            if not isdir(myPath):
                print("ERROR - Invalid path!")
                sys.exit()
    else:
        if not isdir(myPath):
            print("ERROR - Invalid path!")
            sys.exit()

    jobs = Queue()
    files = [f for f in listdir(myPath) if isfile(join(myPath, f))]

    if outputPath == None:
        if "-o" in sys.argv:
            try:
                outputPath = sys.argv[sys.argv.index("-o") + 1]
            except:
                print("ERROR - Invalid output path!")
                sys.exit()
            if not isdir(outputPath):
                print("ERROR - Output directory does not exist!")
                sys.exit()
        else:
            outputPath = None
    else:
        if not isdir(outputPath):
            print("ERROR - Output directory does not exist!")
            sys.exit()

    if numberOfThreads == None:
        if "-t" in sys.argv:
            try:
                RawThreads = sys.argv[sys.argv.index("-t") + 1]
                numberOfThreads = int(RawThreads)
                if numberOfThreads < 1:
                    print("ERROR - Invalid number of threads!")
                    sys.exit()
            except:
                print("ERROR - Invalid number of threads!")
                sys.exit()
        else:
            numberOfThreads = 1
    else:
        try:
            numberOfThreads = int(numberOfThreads)
            if numberOfThreads < 1:
                print("ERROR - Invalid number of threads!")
                sys.exit()
        except:
            numberOfThreads = 1

    def doIt(input, output):
        sig.editMe(input, output)

    def doStuff(q):
        while not q.empty():
            value = q.get()
            doIt(value[0], value[1])
            q.task_done()

    for item in files:
        jobs.put([join(myPath, item), outputPath])

    print("waiting for queue to complete", jobs.qsize(), "tasks")

    for i in range(numberOfThreads):
        t = threading.Thread(target=doStuff, args=(jobs,))
        t.start()

    jobs.join()
    print("all done")

if __name__ == "__main__":
    main()