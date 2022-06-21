import somethongMoreComplicatedWithJson as sig
from queue import Queue
from os import listdir
from os.path import isfile, isdir, join
import multiprocessing as mp
import sys
import time

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

    if outputPath == None:
        if "-o" in sys.argv:
            try:
                outputPath = sys.argv[sys.argv.index("-o") + 1]
            except Exception as e:
                print("ERROR - Invalid output path! Error %s" % e)
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
            except Exception as e:
                print("ERROR - Invalid number of threads! Error %s" % e)
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

    jobs = Queue()
    files = [f for f in listdir(myPath) if isfile(join(myPath, f))]

    def doStuff(q):
        while not q.empty():
            value = q.get()
            print(f"processing {value[0]}")
            sig.editMe(value[0], value[1])
            print(f"finished   {value[0]}")
            q.task_done()

    for item in files:
        jobs.put([join(myPath, item), outputPath])

    print("waiting for queue to complete", jobs.qsize(), "tasks")

    pool = mp.Pool(processes=numberOfThreads)

    for i in range(numberOfThreads):
        t = pool.apply_async(doStuff)
        #t.start()

    jobs.join()
    print("all done")

if __name__ == "__main__":
    t0 = time.time()
    main("//NASJH/Shared/DEVELOP/threading", "//NASJH/Shared/DEVELOP/threading/final/", 20)
    t1 = time.time()
    print("Time taken: %s" % (t1 - t0))