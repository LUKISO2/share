#!/opt/cloudera/parcels/Anaconda/envs/python36/bin/python
import logging.handlers
import configparser
import logging
import pyhocon
import sys
import re
import os

# Hand changable variables
version = '1.2.1'
delmatExtended = []

#temporaly logging before config
formater = logging.Formatter('%(asctime)s %(levelname)s %(name)s$ [%(processName)s] %(message)s')
tmpLog = logging.getLogger('Pre_config_log_of___%s___' % os.path.basename(__file__))
tmpLogHnd = logging.StreamHandler(sys.stderr)
tmpLogHnd.setLevel(logging.ERROR)
tmpLogHnd.setFormatter(formater)
tmpLog.addHandler(tmpLogHnd)

# Get environment variables
config_file = sys.argv[1]
cwd = os.getcwd()
confRoot = os.path.join(os.getenv('DELCHCK_APPL_DIR'), 'config')
logDir = os.path.join(os.getenv('DELCHCK_APPL_DIR'), 'log')
apsEnv = os.getenv('DELCHCK_APS_ENV', 'TEST')

if confRoot is None:
    tmpLog.error('Environment variable CONF_ROOT is not set')
    sys.exit(2)

# Read configuration
if os.path.isfile(os.path.join(confRoot, config_file)):
    config = configparser.ConfigParser()
    config.read(os.path.join(confRoot, config_file))
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else None
    logFile = config.get(apsEnv, 'log_file') if 'log_file' in config.options(apsEnv) else None
else:
    tmpLog.error('Configuration file not found')
    sys.exit(2)

print(f'Log level is {debugLevel}')

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

rotatingHandler = logging.handlers.TimedRotatingFileHandler(os.path.join(logDir, 'app.log'), when='d', interval=30, backupCount=1)
rotatingHandler.setLevel(logging.DEBUG)
rotatingHandler.setFormatter(formater)

logger.addHandler(outHandler)
logger.addHandler(errHandler)
logger.addHandler(rotatingHandler)

# Automatic mail reporting requirement
logger.info(f'Application: delmatchecker.py, Version: {version}, Build: Unknown')

# Loads enviroment vars from config file
def loadEnviron(config):
    toReturn = []
    if not os.path.isfile(config):
        logger.warn(f'{config} Does not exist')
        return toReturn
    try:
        with open(config, 'r') as conf:
            for line in conf.readlines():
                if '=' in line:
                    key, value = line.split('=')
                    key = key.strip().replace('export ', '')
                    value = value.strip()
                    value = os.path.expandvars(value)
                    value = value.replace('"', '').replace('\\', '/').replace('//', '/')
                    os.environ[key] = str(value)
                    logger.debug(f'LOADED: "{key}={value}"')
                    toReturn.append([key, value])
    except Exception as e:
        logger.error(f'{config} Failed to load: {e}')
    return toReturn

# Sets basic and missing env variables
os.environ['CONF_DIR'] = os.environ['CONF_ROOT']
logger.debug(f'LOADED: CONF_DIR={os.environ["CONF_DIR"]}')
os.environ['COMMONCONF_DIR'] = f'{os.environ["CONF_ROOT"]}/common'
logger.debug(f'LOADED: COMMONCONF_DIR={os.environ["COMMONCONF_DIR"]}')
os.environ['SQL_DIR'] = f'{os.environ["CONF_ROOT"]}/sql'
logger.debug(f'LOADED: SQL_DIR={os.environ["SQL_DIR"]}')
os.environ['COMMONSQL_DIR'] = f'{os.environ["SQL_DIR"]}/common'
logger.debug(f'LOADED: COMMONSQL_DIR={os.environ["COMMONSQL_DIR"]}')

delmatFolder = os.environ['APPL_DIR'].replace('DATAINGEST', 'DELMAT')

def check(checki):
    for item in checki:
        if item[0] == 'config_File':
            logger.debug(f'Found another config file contained in a config file: {item[1]}')
            check(loadEnviron(item[1]))
check(loadEnviron(os.path.join(os.environ['COMMONCONF_DIR'], f'general_{os.environ["DATAINGEST_ENV"]}.conf')))

# Data processing
# Loads all paths that DELMAT periodically checks
for item in os.listdir(delmatFolder):
    if os.path.isdir(os.path.join(delmatFolder, item)):
        logger.debug(f'Found delmat folder: {item}')
        pathe = os.path.join(delmatFolder, item)
        for item2 in os.listdir(pathe):
            path = os.path.join(pathe, item2)
            if not os.path.isfile(path):
                continue
            try:
                with open(path, 'r') as f:
                    for line in f.readlines():
                        if '=' in line:
                            splited = line.split('=')
                            key = splited[0].strip()
                            value = splited[-1].strip()
                            value = os.path.expandvars(value)
                            value = value.replace('"', '').replace('\\', '/').replace('//', '/')
                            if key == 'HDFS_FEED_DIRECTORY':
                                delmatExtended.append([os.path.normpath(value), os.path.join(os.path.basename(os.path.dirname(path)), os.path.basename(path))])
                                logger.debug(f'Found DELMAT folder: {delmatExtended[-1][0]}, in {delmatExtended[-1][1]}')
            except Exception as e:
                logger.error(f'Failed to open DELMAT {path}')

# Loads all files and filters out the relevant ones
for item in os.listdir(os.environ['CONF_ROOT']):
    logger.debug(f'Loading {item}')
    if os.path.isdir(os.path.join(os.environ['CONF_ROOT'], item)):
        logger.debug(f'Working with folder {item}')
        pathe = os.path.join(os.environ['CONF_ROOT'], item)
        for item2 in os.listdir(pathe):
            configFile = os.path.join(pathe, item2)
            if not os.path.isfile(configFile):
                continue
            if not configFile.endswith('.conf') or re.search('jaas.*.conf', configFile):
                continue
            
            # Loads required variables into env to be used by parser and logging
            logger.info('Loading config file: ' + configFile)
            try:
                config = pyhocon.ConfigFactory.parse_file(configFile)
            except Exception as e:
                logger.warn(f"{configFile} is either not a valid hocon config file or pyhocon wasn't able to resolve a global variable, error: {e}")
                continue
            os.environ['feed_System'] = str(config.get('feed_System', None))
            if os.environ['feed_System'] is None:
                logger.warn('No feed_System defined in config file: ' + configFile)
                continue
            os.environ['feed_Name'] = str(config.get('feed_Name', None))
            if os.environ['feed_Name'] is None:
                logger.warn('No feed_Name defined in config file: ' + configFile)
                continue
            os.environ['feed_Version'] = str(config.get('feed_Version', None))
            if os.environ['feed_Version'] is None:
                logger.warn('No feed_Version defined in config file: ' + configFile)
                continue
            direction = config.get('feed_Direction', None)
            logger.info(f'feed_Direction: {str(direction)}, feed_System: {os.environ["feed_System"]}, feed_Name: {os.environ["feed_Name"]}, feed_Version: {os.environ["feed_Version"]}')
            failMail = config.get('feed_FailMail', None)
            logger.info('feed_FailMail: ' + str(failMail))
            author = config.get('feed_Author', 'Unknown')
            logger.info('feed_Author: ' + str(author))

            # Loads input from files, check for LocalFS input_Type and if they archive to HDFS, if so it check for their existence in DELMAT, raises error if not found
            input = config.get('input.input_Type', None)
            if input == 'LocalFS':
                inputHDFS = config.get('input.archive_HDFS_Path', None)
                if inputHDFS is None:
                    continue
                delmatToCheckPath = os.path.normpath(os.path.expandvars(inputHDFS).replace('"', '').replace('\\', '/').replace('//', '/').split('/p_dt')[0])
                logger.debug(f'Input HDFS path: {delmatToCheckPath}')
                passedIn = False
                for deli in delmatExtended:
                    if re.search(deli[0], delmatToCheckPath):
                        logger.info(f'Input config {configFile} found in delmat {deli[1]}')
                        passedIn = True
                        break
                if not passedIn:
                    logger.error(f'Input HDFS path {delmatToCheckPath} from {configFile} is not in delmat files, file made by: {author}')

            # Does basically the same as input, but checkes ALL outputs for HDFS and raises error if path not found in DELMAT
            outputs = config.get('output', [])
            for output in outputs:
                outputType = output.get('output_Type', None)
                if outputType == 'HDFS':
                    outputHDFS = config.get('output.output_HDFS_Path', None)
                    if outputHDFS is None:
                        continue
                    hdfsPath = os.path.normpath(os.path.expandvars(outputHDFS).replace('"', '').replace('\\', '/').replace('//', '/'))
                    logger.debug(f'Output hadoop path: {hdfsPath}')
                    passedOut = False
                    for deli in delmatExtended:
                        if re.search(deli[0], hdfsPath):
                            logger.info(f'Output config {configFile} found in delmat {deli[1]}')
                            passedOut = True
                            break
                    if not passedOut:
                        logger.error(f'Output HDFS path {hdfsPath} from {configFile} is not in delmat files, file made by: {author}')
