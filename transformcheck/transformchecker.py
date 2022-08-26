#!/opt/cloudera/parcels/Anaconda/envs/python36/bin/python
from datetime import datetime
import logging.handlers
import configparser
import subprocess
import logging
import time
import sys
import re
import os

# Hand changable variables
version = '2.1.1'
unsortedFinalPaths = []
finalPaths = []
pathway = []
toDel = []

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
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else None # Can be [debug, info, warning, error, critical]
    logFile = config.get(apsEnv, 'log_file') if 'log_file' in config.options(apsEnv) else None # Should be only a file name
    mainPath = config.get(apsEnv, 'hdfs_path') if 'hdfs_path' in config.options(apsEnv) else None # Where to check for old files on HDFS
    age = config.get(apsEnv, 'age') if 'age' in config.options(apsEnv) else None # Files older than this will be WARNed instead of INFOed; int in seconds
    deletePendingAge = config.get(apsEnv, 'delete_pending_age') if 'delete_pending_age' in config.options(apsEnv) else None # Files and folders in pending fodler older than this will be deleted from HDFS; int in seconds
    deleteRejectAge = config.get(apsEnv, 'delete_reject_age') if 'delete_reject_age' in config.options(apsEnv) else None # Files and folders in reject folder older than this will be deleted from HDFS; int in seconds
else:
    tmpLog.error('Configuration file not found')
    sys.exit(2)

if None in [debugLevel, logFile, age, mainPath, deletePendingAge, deleteRejectAge]:
    tmpLog.error('Missing required configuration options')
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

try:
    age = int(age)
    deletePendingAge = int(deletePendingAge)
    deleteRejectAge = int(deleteRejectAge)
except ValueError:
    logger.error('Invalid config: "*age": make sure it is an integer value in seconds!')
    sys.exit(2)

# Loads all files and filters out the relevant ones
folders = subprocess.run(f"hdfs dfs -ls -R {mainPath} | grep '^d' | grep -e '/pending/' -e '/reject/'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
ansInp = folders.stdout.decode('utf-8').strip()

# Checks if the files are older than the age variable and logs them
def main(line, age=age, deletePendingAge=deletePendingAge, deleteRejectAge=deleteRejectAge):
    logger.debug(f'Checking {line}')
    lines = line.strip().split(' ')
    timen = f'{lines[-3]} {lines[-2]}'
    
    logger.debug(f'Parsing time: {timen}')
    try:
        estamp = time.mktime(datetime.strptime(timen, '%Y-%m-%d %H:%M').timetuple())
    except ValueError:
        logger.error(f'Invalid time format found: {timen} in line: {line}')
        return
    
    if estamp < time.time() - deletePendingAge and re.search('/pending/', line[-1]):
        logger.info(f'Path from pending is older than {deletePendingAge} seconds, added to delete list: {lines[-1]}')
        return [lines[-1]]
    if estamp < time.time() - deleteRejectAge and re.search('/reject/', lines[-1]):
        logger.info(f'Parh from reject is older than {deleteRejectAge} seconds, added to delete list: {lines[-1]}')
        return [lines[-1]]
    if estamp > time.time() - age:
        logger.debug(f'Skipping path because its too new: {lines[-1]}')
        return

    logger.warn(f'Found old folder: {lines[-1]}')

for line in ansInp.split('\n'):
    returned = main(line)
    if returned is not None:
        toDel.append(returned)

if len(toDel) > 0:
    logger.info(f'Found {len(toDel)} old folders, deleting...')
    subprocess.run(f"hdfs dfs -rm -r {' '.join(toDel)}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for item in toDel:
        logger.debug(f'Deleted {item}')
    logger.info('Delete complete')

logger.info('DONE!')