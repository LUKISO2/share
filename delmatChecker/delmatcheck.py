#!/opt/cloudera/parcels/Anaconda/envs/python36/bin/python
import concurrent.futures
import logging.handlers
import configparser
import subprocess
import logging
import queue
import pyhocon
import sys
import re
import io
import os

# Hand changable variables
version = '1.2.1'
delmatExtended = []
toDo = queue.Queue()

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
    if os.path.isdir(os.path.join(os.environ['CONF_ROOT'], item)):
        pathe = os.path.join(os.environ['CONF_ROOT'], item)
        for item2 in os.listdir(pathe):
            configFile = os.path.join(pathe, item2)
            if not os.path.isfile(configFile):
                continue
            if not configFile.endswith('.conf') or re.search('jaas.*.conf', configFile):
                continue
            toDo.put(configFile)

def main(configFile, delmatExtended=delmatExtended, logger=logger):
        # Loads required variables into env to be used by parser and logging
        logger.info('Loading config file: ' + configFile)
        try:
            config = pyhocon.ConfigFactory.parse_file(configFile)
        except Exception as e:
            logger.warn(f"{configFile} is either not a valid hocon config file or pyhocon wasn't able to resolve a global variable, error: {e}")
            return
        feedSystem = str(config.get('feed_System', None))
        os.environ['feed_System'] = feedSystem
        if feedSystem is None:
            logger.warn('No feed_System defined in config file: ' + configFile)
            return
        feedName = str(config.get('feed_Name', None))
        os.environ['feed_Name'] = feedName
        if feedName is None:
            logger.warn('No feed_Name defined in config file: ' + configFile)
            return
        feedVersion = str(config.get('feed_Version', None))
        os.environ['feed_Version'] = feedVersion
        if feedVersion is None:
            logger.warn('No feed_Version defined in config file: ' + configFile)
            return
        direction = config.get('feed_Direction', None)
        logger.info(f'feed_Direction: {str(direction)}, feed_System: {feedSystem}, feed_Name: {feedName}, feed_Version: {feedVersion}')
        failMail = config.get('feed_FailMail', None)
        logger.info('feed_FailMail: ' + str(failMail))
        author = config.get('feed_Author', 'Unknown')
        logger.info('feed_Author: ' + str(author))

        # Loads input from files, check for LocalFS input_Type and if they archive to HDFS, if so it check for their existence in DELMAT, raises error if not found
        inputHDFS = config.get('input.archive_HDFS_Path', None)
        if inputHDFS is None:
            return
        delmatToCheckPath = os.path.normpath(os.path.expandvars(inputHDFS).replace('"', '').replace('\\', '/').replace('//', '/').split('/p_dt')[0])
        logger.debug(f'Input HDFS path: {delmatToCheckPath}')
        answerInp = subprocess.run(['hadoop', 'fs', '-ls', delmatToCheckPath], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ansInp = answerInp.stdout.decode('utf-8').strip()
        passedIn = False
        if not re.search('/p_', ansInp, flags=re.M|re.S):
            logger.info(f'No p_* folder found on hadoop path: {ansInp}')
            return
        for deli in delmatExtended:
            if re.search(deli[0], delmatToCheckPath):
                logger.info(f'Input config {configFile} found in delmat {deli[1]}')
                passedIn = True
                break
        if not passedIn:
            logger.warn(f'Input HDFS path {delmatToCheckPath} from {configFile} is not in delmat files, file made by: {author}')

        # Does basically the same as input, but checkes ALL outputs for HDFS and raises error if path not found in DELMAT
        outputs = config.get('output', [])
        for output in outputs:
            outputType = output.get('output_Type', None)
            if outputType == 'HDFS':
                outputHDFS = config.get('output.output_HDFS_Path', None)
                if outputHDFS is None:
                    return
                hdfsPath = os.path.normpath(os.path.expandvars(outputHDFS).replace('"', '').replace('\\', '/').replace('//', '/'))
                logger.debug(f'Output hadoop path: {hdfsPath}')
                answerOut = subprocess.run(['hadoop', 'fs', '-ls', delmatToCheckPath], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                ansOut = answerOut.stdout.decode('utf-8').strip()
                if not re.search('/p_', ansOut, flags=re.M|re.S):
                    logger.info(f'No p_* folder found on hadoop path: {ansOut}')
                    return
                passedOut = False
                for deli in delmatExtended:
                    if re.search(deli[0], hdfsPath):
                        logger.info(f'Output config {configFile} found in delmat {deli[1]}')
                        passedOut = True
                        break
                if not passedOut:
                    logger.warn(f'Output HDFS path {hdfsPath} from {configFile} is not in delmat files, file made by: {author}')

if __name__ == '__main__':
    tre = []
    for item in range(toDo.qsize()):
        tre.append(toDo.get())

    # BEFORE SETTING max_workers TO ANYTHING BUT 1, you must rewrite file Handlerel to io.SringIO var!
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(main, tre)

logger.info('DONE!')
