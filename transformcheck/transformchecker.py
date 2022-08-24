#!/opt/cloudera/parcels/Anaconda/envs/python36/bin/python
from datetime import datetime
import logging.handlers
import configparser
import subprocess
import logging
import time
import sys
import os

# Hand changable variables
version = '2.0.0'
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
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else None # Can be [debug, info, warning, error, critical]
    logFile = config.get(apsEnv, 'log_file') if 'log_file' in config.options(apsEnv) else None # Should be only a file name
    age = config.get(apsEnv, 'age') if 'age' in config.options(apsEnv) else None # Files older than this will be WARNed instead of INFOed; int in seconds
    mainPath = config.get(apsEnv, 'hdfs_path') if 'hdfs_path' in config.options(apsEnv) else None
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

try: age = int(age)
except ValueError:
    logger.error('Invalid file age, make sure it is an integer value in seconds!')
    sys.exit(2)

# Loads all files and filters out the relevant ones
folders = subprocess.run(f"hdfs dfs -ls -R {mainPath} | grep '^d' | grep -e '/pending/' -e '/reject/'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
ansInp = folders.stdout.decode('utf-8').strip()

for line in ansInp.split('\n'):
    lines = line.split(' ')
    finalPaths.append(lines[-1])
finalPaths = [*set(os.path.dirname(x) for x in finalPaths)]

# Checks if the files are older than the age variable and logs them
def main(line, age=age):
    lines = line.strip().split(' ')
    timen = f'{lines[16]} {lines[17]}'
    logger.debug(f'Parsing time: {timen}')
    estamp = time.mktime(datetime.strptime(timen, '%Y-%m-%d %H:%M').timetuple())
    if estamp > time.time() - age:
        logger.debug(f'Skipping path because its too new: {lines[-1]}')
        return
    logger.warn(f'Found old folder with files: {lines[-1]}')

# Prepares paths to be checked
datesr = subprocess.run(f"hdfs dfs -ls -R {mainPath} | grep -E '{'$|'.join(finalPaths)}$'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
dates = datesr.stdout.decode('utf-8').strip()
for line in dates.split('\n'):
    main(line)

logger.info('DONE!')