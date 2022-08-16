# Version 1.0.1

from datetime import datetime
import configparser
import argparse
import logging
import time
import sys
import re
import os

#Basic Values
matched = 0
mainFormat = "%Y%m%d%H%M%S"

#temporaly logging before config
tmpLog = logging.getLogger(f'Pre_config_log_of___{os.path.basename(__file__)}___')
tmpLogHnd = logging.StreamHandler(sys.stderr)
tmpLogHnd.setLevel(logging.ERROR)
tmpLogHnd.setFormatter(logging.Formatter('%(asctime)s %(levelname)s; %(message)s'))
tmpLog.addHandler(tmpLogHnd)

#getting filename argument
argparser = argparse.ArgumentParser(description='Renames tmp files')
argparser.add_argument('config_name', type=str, help='Name of the config file', nargs=1)
config_file = argparser.parse_args().config_name[0]

# Get environment variables
confRoot = os.getenv('CONF_ROOT')
apsEnv = os.getenv('APS_ENV','TEST')

if confRoot is None:
    tmpLog.error('Environment variable CONF_ROOT is not set')
    sys.exit(2)

# Read configuration
if os.path.isfile(os.path.join(confRoot, config_file)):
    config = configparser.RawConfigParser()
    config.read(os.path.join(confRoot, config_file))
    inputDir = config.get(apsEnv, 'input_dir') if 'input_dir' in config.options(apsEnv) else None
    fileMask = config.get(apsEnv, 'inp_file_mask') if 'inp_file_mask' in config.options(apsEnv) else None
    stampMask = config.get(apsEnv, 'timestamp_regexp') if 'timestamp_regexp' in config.options(apsEnv) else None
    outputFileRaw = config.get(apsEnv, 'out_file') if 'out_file' in config.options(apsEnv) else None
    uniFormat = config.get(apsEnv, 'time_format') if 'time_format' in config.options(apsEnv) else None
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else 'DEBUG'
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
    tmpLog.error('Invalid debug level')
    sys.exit(2)

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(MIN_LEVEL)

formater = logging.Formatter('%(asctime)s %(levelname)s; %(message)s')

outHandler = logging.StreamHandler(sys.stdout)
outHandler.setFormatter(formater)

lowerThanError = MaxLevelFilter(logging.ERROR)
outHandler.addFilter(lowerThanError)
outHandler.setLevel(MIN_LEVEL)

errHandler = logging.StreamHandler(sys.stderr)
errHandler.setLevel(logging.ERROR)
errHandler.setFormatter(formater)

errHandler.setLevel(max(MIN_LEVEL, logging.ERROR))

logger.addHandler(outHandler)
logger.addHandler(errHandler)

#Logging
logger.debug(f'Enviroment is set to: {apsEnv}')
logger.debug(f'Debug level: {debugLevel}')
logger.debug(f'Input directory: {inputDir}')
logger.debug(f'File mask: {fileMask}')
logger.debug(f'Timestamp mask: {stampMask}')
logger.debug(f'Time format: {uniFormat}')

outputFile = f'{str(outputFileRaw)}_{str(datetime.now().strftime(mainFormat))}{str(os.getpid())}.list'

#Checks if the config file loaded correctly
if None in [inputDir, fileMask, debugLevel, stampMask, outputFile, uniFormat]:
    logger.error("Missing configuration parameters")
    sys.exit(2)

#Checks existence of parameters
if not os.path.isdir(inputDir):
    logger.error("Input directory doesn't exist")
    sys.exit(2)
if len(os.listdir(inputDir)) == 0:
    logger.error("Input directory is empty")
    sys.exit(1)
if not os.path.isfile(outputFile):
    if not os.path.isdir(os.path.dirname(outputFile)):
        logger.error('Folder of the output file does not exist')
        sys.exit(2)
    with open(outputFile, 'w') as f:
        f.write('')
        f.close()
try: re.compile(fileMask)
except re.error:
    logger.error("Invalid file mask")
    sys.exit(2)
try: re.compile(stampMask)
except re.error:
    logger.error("Invalid timestamp mask")
    sys.exit(2)

logger.debug('----------')

#Matching files
for file in os.listdir(inputDir):
    if not re.match(fileMask, file):
        logger.debug(f'Failed matching: {file}')
        continue
    logger.info(f'Match found: {file}')

    #extracts timestamp from filename
    fstamp = re.findall(stampMask, file)
    if len(fstamp) == 1:
        estamp = time.mktime(datetime.strptime(fstamp[0], uniFormat).timetuple())
        logger.debug(f'filename timestamp praised to: {estamp}')
    else:
        logger.error(f'filename timestamp not found, or found multiple times in: {file}')
        logger.error(f'filename timestamps found: {fstamp}')
        sys.exit(2)

    #Gets timestamp from file
    mstamp = os.path.getmtime(os.path.join(inputDir, file))
    logger.debug(f'File modification timestamp: {mstamp}')

    #Checks for validity of timestamps
    if estamp > mstamp:
        logger.error(f'File modification timestamp is older than filename timestamp in: {file}')
        sys.exit(2)

    difference = round(mstamp - estamp)
    logger.info(f'File modification timestamp is {difference} seconds older than filename timestamp in: {file}')

    #Appends timestamp to filename
    with open(outputFile, 'a') as f:
        f.write(f'{file}|{fstamp[0]}|{datetime.fromtimestamp(mstamp).strftime(mainFormat)}|{difference}\n')
        f.close()
    
    matched += 1

logger.debug('----------')

if matched == 0:
    logger.error("No files matched")
    sys.exit(1)

logger.info(f'DONE - {matched} files matched and were appended to {outputFile}')