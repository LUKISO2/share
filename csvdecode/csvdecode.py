import sys
import os.path

# Basic values
numberOfFields = 0
nowProcesing = 0
config = {}

#Checks for config and opens it
if os.path.isfile(sys.argv[-1]):
    configFile = open(sys.argv[-1]).read()
else:
    print("ERROR - Config file does not exists!")
    exit(1)

# Checking for "all" argument
if "-a" in sys.argv:
    printAll = True
else:
    printAll = False

# Checking for special delimiter, default is "|"
if "-d" in sys.argv:
    delimiterRaw = sys.argv[sys.argv.index("-d") + 1]
    if len(delimiterRaw) == 1:
        delimiter = delimiterRaw
    try:
        if len(delimiterRaw) == 3:
            delimiter = bytearray.fromhex(delimiterRaw.strip("\\")).decode()
        if len(delimiterRaw) == 4:
            delimiter = chr(int(delimiterRaw.strip("\\")), 8)
    except:
        print("ERROR - Invalid delimiter!")
        exit(1)
    if len(delimiterRaw) > 5 or len(delimiterRaw) == 0 or len(delimiterRaw) == 2:
        print("ERROR - Invalid delimiter!")
        exit(1)
else:
    delimiter = "|"

# Checking for file to decode
if "-f" in sys.argv:
    fileToDecode = sys.argv[sys.argv.index("-f") + 1]
    if not os.path.isfile(fileToDecode):
        print("ERROR - File to decode does not exists!")
        exit(1)
    decodableFile = True
else:
    decodableFile = False

# Making config
for line in configFile.split('\n'):
    if line.startswith('<') or line == '':
        continue
    tmp = line.strip().split(' ')
    tmp2 = [tmp[0], tmp[-1]]
    numberOfFields += 1
    config[numberOfFields] = tmp2

# The real work being done here
def sawTogether(nowProcesing, config, printAll, delimiter, stdin):
    for line in stdin.split('\n'):
        nowProcesing += 1
        if line == '':
            return nowProcesing
        inputList = line.strip().split(delimiter)
        print("# Record %s -------------------------------------------------------" % nowProcesing)
        nowProcesingLine = 0
        try:
            for object in range(len(inputList)-1):
                nowProcesingLine += 1
                if inputList[object] == '' and printAll == False:
                    continue
                if config[object+1][1] == "int":
                    print('%s. %s = %s' %(nowProcesingLine, config[object+1][0], inputList[object]))
                    sys.stdout.flush()
                else:
                    print('%s. %s = "%s"' %(nowProcesingLine, config[object+1][0], inputList[object]))
                    sys.stdout.flush()
            return nowProcesing
        except:
            print('*****\nERROR - Input %s was invali! Thanks to that "Record %s" can be wrong!\n%s\n*****' %(nowProcesing, nowProcesing, inputList))
            return nowProcesing

# Checking for decoutable stuff
if decodableFile == True:
    fileLines = open(fileToDecode).read()
    for stdin in fileLines.split('\n'):
        nowProcesing = sawTogether(nowProcesing, config, printAll, delimiter, stdin)
else:
    for stdin in sys.stdin:
        nowProcesing = sawTogether(nowProcesing, config, printAll, delimiter, stdin)