#!/opt/cloudera/parcels/Anaconda/envs/python36/bin/python
from itertools import groupby
import logging.handlers
import configparser
import subprocess
import logging
import sys
import re
import os

# Hand changable variables
version = '0.0.1'
mainPath = '/user/havlasekj/data/transform'
unsortedFinalPaths = []
finalPaths = []
pathway = []

#temporaly logging before config
formater = logging.Formatter('%(asctime)s %(name)s$ [%(thread)d] %(levelname)s %(message)s')
tmpLog = logging.getLogger(f'Pre config log of {os.path.basename(__file__)}')
tmpLogHnd = logging.StreamHandler(sys.stderr)
tmpLogHnd.setLevel(logging.ERROR)
tmpLogHnd.setFormatter(formater)
tmpLog.addHandler(tmpLogHnd)

# Get environment variables
config_file = sys.argv[1]
confRoot = os.path.join(os.getenv('APPL_DIR'), 'config')
logDir = os.path.join(os.getenv('APPL_DIR'), 'log')
apsEnv = os.getenv('APS_ENV', 'TEST')

if confRoot is None:
    tmpLog.error('Environment variable CONF_ROOT is not set')
    sys.exit(2)

# Read configuration
if os.path.isfile(os.path.join(confRoot, config_file)):
    config = configparser.ConfigParser()
    config.read(os.path.join(confRoot, config_file))
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else None
    logFile = config.get(apsEnv, 'log_file') if 'log_file' in config.options(apsEnv) else None
    age = config.get(apsEnv, 'age') if 'age' in config.options(apsEnv) else None
else:
    tmpLog.error('Configuration file not found')
    sys.exit(2)

# Set logging
class MaxLevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno < self.level

try:
    MIN_LEVEL = getattr(logging, debugLevel.upper())
except AttributeError:
    logging.error('Invalid debug level')
    sys.exit(2)

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(MIN_LEVEL)

outHandler = logging.StreamHandler(sys.stdout)
outHandler.setFormatter(formater)

lowerThanError = MaxLevelFilter(logging.ERROR)
outHandler.addFilter(lowerThanError)
outHandler.setLevel(MIN_LEVEL)

errHandler = logging.StreamHandler(sys.stderr)
errHandler.setLevel(logging.ERROR)
errHandler.setFormatter(formater)
errHandler.setLevel(max(MIN_LEVEL, logging.ERROR))

rotatingHandler = logging.handlers.TimedRotatingFileHandler(os.path.join(logDir, logFile), when='d', interval=30, backupCount=1)
rotatingHandler.setLevel(logging.DEBUG)
rotatingHandler.setFormatter(formater)

logger.addHandler(outHandler)
logger.addHandler(errHandler)
logger.addHandler(rotatingHandler)

# Automatic mail reporting requirement
logger.info(f'Application: transformchecker.py, Version: {version}, Build: Unknown')

# Loads all files and filters out the relevant ones
folders = subprocess.run("hdfs dfs -ls -R %s | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'" % mainPath, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
ansInp = folders.stdout.decode('utf-8').strip()

def countIt(stringr):
    stringy = stringr.strip()
    string = stringy.replace('|', '')
    groups = groupby(string)
    result = [(label, sum(1 for _ in group)) for label, group in groups]
    return result[0][1] if result[0][0] == '-' else 0

for x, line in enumerate(ansInp.split('\n')):
    if x == 0:
        if not line.strip().startswith('|'):
            logger.error(f'Unble to parse the output of: "hdfs dfs -ls -R {mainPath}", the program/user is most likely missing permissions to read the folder: {line}')
            sys.exit(2)
        delNumber = countIt(line)
        todelete = f'|{delNumber*"-"}'
        base = [0, mainPath]
        pathway.append(mainPath)
    if not line.strip().startswith('|'):
        logger.warning(f'Unable to parse line {x+1}: {line}')
        continue
    line = line.strip().replace(todelete, '')
    dashes = countIt(line)
    eline = line.replace(dashes*'-', '')
    replacable = dashes//2
    logger.debug(f'Parsing line on level {replacable}: {eline}')
    if replacable > 3:
        logger.info(f'Skipping line with data from too deep {x+1}: {eline}')
        continue
    if replacable == 0:
        pathway = [mainPath, eline]
        unsortedFinalPaths.append(os.path.join(*pathway))
        logger.debug(f'On base {base[0]} added {base[1]}')
        continue
    if replacable == base[0]:
        pathway = [*pathway[:replacable+1], eline]
        unsortedFinalPaths.append(os.path.join(*pathway))
        logger.debug(f'On base {base[0]} added {os.path.join(*pathway)}')
        continue
    if replacable > base[0]:
        if replacable-base[0] != 1:
            logger.error(f'Somehow a large problem occured, the tree reports a 2 folder jump!')
            sys.exit(2)
        pathway = [*pathway, eline]
        unsortedFinalPaths.append(os.path.join(*pathway))
        base = [replacable, os.path.join(*pathway)]
        logger.debug(f'On base {base[0]} added {base[1]}')
        continue
    if replacable < base[0]:
        pathway = [*pathway[:replacable+1], eline]
        unsortedFinalPaths.append(os.path.join(*pathway))
        base = [replacable, os.path.join(*pathway)]
        logger.debug(f'On base {base[0]} added path {base[1]}')
        continue

def main(pathCheck, age=age):
    pass

for path in unsortedFinalPaths:
    if not re.search('/pending/', path) and not re.search('/reject/', path):
        logger.debug(f'Skipping path because its mising pending/reject in it: {path}')
        continue
    logger.info(f'Checking path: {path}')
    finalPaths.append(path)

logger.debug(f'Final paths: {finalPaths}')
finalPaths = [*set(os.path.dirname(x) for x in finalPaths)]
logger.debug(f'Finallly final paths: {finalPaths}')

logger.info('DONE!')