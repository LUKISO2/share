#!/opt/cloudera/parcels/Anaconda/envs/python36/bin/python
import logging.handlers
import configparser
import subprocess
import pyhocon
import logging
import queue
import json
import sys
import re
import os

# WHAT THIS SCRIPT DOES AND WHAT IT REQUIRES
# It requires a config file name as a argument and delmat environment variables to be set
# Example of a config file:
#
# [TEST]
# debug_level = DEBUG <- (NOT required, default: INFO)
# log_name = delmatChecker <- (NOT required, default: delmatChecker)
# json_name = delmatChecker.json <- (NOT required, default: delmatChecker.json)
# path_hdfs = /user/ingest/delmatChecker <- (NOT required, default: None)
#
# details of the config file below...

# Hand changable variables
version = '1.3.0'
delmatExtended = []
toDo = queue.Queue()

#temporaly logging before config
formater = logging.Formatter('%(asctime)s %(name)s$ [%(thread)d] %(levelname)s %(message)s')
tmpLog = logging.getLogger(f'Pre_config_log_of___{os.path.basename(__file__)}___')
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
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else 'INFO' # Debug level (DEBUG -> CRITICAL)
    logFile = config.get(apsEnv, 'log_name') if 'log_name' in config.options(apsEnv) else 'delmatChecker' # Log file name (NO EXTENSION!)
    jsonFile = config.get(apsEnv, 'json_name') if 'json_name' in config.options(apsEnv) else None # Json name if different or absolute path to file to write to (does not need to exist)
    pathHdfs = config.get(apsEnv, 'path_hdfs') if 'path_hdfs' in config.options(apsEnv) else None # HDFS path to copy json file to
else:
    tmpLog.error('Configuration file not found')
    sys.exit(2)

# Json file name handling
if not jsonFile:
    jsonFile = os.path.join(logDir, logFile + '.json')
elif not jsonFile.endswith('.json'):
    jsonFile = jsonFile + '.json'
elif not os.path.isabs(jsonFile):
    jsonFile = os.path.join(logDir, jsonFile)

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

rotatingHandler = logging.handlers.TimedRotatingFileHandler(os.path.join(logDir, logFile + '.log'), when='d', interval=30, backupCount=1)
rotatingHandler.setLevel(MIN_LEVEL)
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
                                logger.info(f'Found DELMAT path in file: {delmatExtended[-1][0]}, in {delmatExtended[-1][1]}')
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

allLogs = []
rex = re.compile('/p_', flags=re.M|re.S)

def main(configFile, delmatExtended=delmatExtended, logger=logger):
    # Loads required variables into env to be used by parser and logging
    debug = []
    debug.append([logging.INFO, 'Working with config file: ' + configFile])
    try:
        config = pyhocon.ConfigFactory.parse_file(configFile)
    except Exception as e:
        debug.append([logging.WARN, f"{configFile} is either not a valid hocon config file or pyhocon wasn't able to resolve a global variable, error: {e}"])
        for toLog in debug:
            logger.log(toLog[0], toLog[1])
        return
    feedSystem = str(config.get('feed_System', None))
    if feedSystem is None:
        debug.append([logging.WARN, 'No feed_System defined in config file: ' + configFile])
        for toLog in debug:
            logger.log(toLog[0], toLog[1])
        return
    feedName = str(config.get('feed_Name', None))
    if feedName is None:
        debug.append([logging.WARN, 'No feed_Name defined in config file: ' + configFile])
        for toLog in debug:
            logger.log(toLog[0], toLog[1])
        return
    feedVersion = str(config.get('feed_Version', None))
    if feedVersion is None:
        debug.append([logging.WARN, 'No feed_Version defined in config file: ' + configFile])
        for toLog in debug:
            logger.log(toLog[0], toLog[1])
        return
    direction = config.get('feed_Direction', None)
    failMail = config.get('feed_FailMail', None)
    author = config.get('feed_Author', 'Unknown')
    debug.append([logging.INFO, f'feed_Direction: {str(direction)}, feed_System: {feedSystem}, feed_Name: {feedName}, feed_Version: {feedVersion}, feed_FailMail: {failMail}, feed_Author: {author}'])

    # Loads input from files, check for LocalFS input_Type and if they archive to HDFS, if so it check for their existence in DELMAT, raises error if not found
    inputHDFS = config.get('input.archive_HDFS_Path', None)
    if inputHDFS is not None:
        delmatToCheckPath = os.path.normpath(os.path.expandvars(inputHDFS).replace('"', '').replace('\\', '/').replace('//', '/').split('/p_')[0].replace(r'${feed_System}', feedSystem).replace(r'${feed_Name}', feedName).replace(r'${feed_Version}', feedVersion))
        debug.append([logging.DEBUG, f'Input HDFS path: {delmatToCheckPath}'])
        answerInp = subprocess.run(['hadoop', 'fs', '-ls', delmatToCheckPath], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ansInp = answerInp.stdout.decode('utf-8').strip()
        passedIn = False
        if not rex.search(ansInp):
            debug.append([logging.DEBUG, f'No input p_* folder found on HDFS path: {ansInp}'])
            debug.append([logging.INFO, f'No input p_* folder found on: {delmatToCheckPath}, skipping'])
        else:
            for deli in delmatExtended:
                if re.search(deli[0], delmatToCheckPath):
                    debug.append([logging.INFO, f'Input config {configFile} found in delmat {deli[1]}'])
                    passedIn = True
                    break
            if not passedIn:
                debug.append([logging.WARN, f'Input HDFS path {delmatToCheckPath} from {configFile} is not in delmat files, file made by: {author}'])
                folderSize = subprocess.run(f'hdfs dfs -du {os.path.dirname(delmatToCheckPath)} | grep -e "{delmatToCheckPath}"', shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).split()[0]
                allLogs.append({'configFile': configFile, 'delmat': delmatToCheckPath, 'type': 'input', 'feedAuthor': author, 'feedFailMail': failMail, 'feedDirection': direction, 'feedSystem': feedSystem, 'feedName': feedName, 'feedVersion': feedVersion, 'folderSize': folderSize})

    # Does basically the same as input, but checkes ALL outputs for HDFS and raises error if path not found in DELMAT
    outputs = config.get('output', [])
    for output in outputs:
        outputType = output.get('output_Type', None)
        if outputType == 'HDFS':
            outputHDFS = output.get('output_Path', None)
            if outputHDFS is None:
                continue
            hdfsPath = os.path.normpath(os.path.expandvars(outputHDFS).replace('"', '').replace('\\', '/').replace('//', '/').replace(r'${feed_System}', feedSystem).replace(r'${feed_Name}', feedName).replace(r'${feed_Version}', feedVersion))
            debug.append([logging.DEBUG, f'Output HDFS path: {hdfsPath}'])
            answerOut = subprocess.run(['hadoop', 'fs', '-ls', hdfsPath], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            ansOut = answerOut.stdout.decode('utf-8').strip()
            if not re.search(ansOut):
                debug.append([logging.DEBUG, f'No output p_* folder found on HDFS path: {ansOut}'])
                debug.append([logging.INFO, 'No output p_* folder found, skipping'])
                continue
            passedOut = False
            for deli in delmatExtended:
                if re.search(deli[0], hdfsPath):
                    debug.append([logging.INFO, f'Output config {configFile} found in delmat {deli[1]}'])
                    passedOut = True
                    break
            if not passedOut:
                debug.append([logging.WARN, f'Output HDFS path {hdfsPath} from {configFile} is not in delmat files, file made by: {author}'])
                folderSize = subprocess.run(f'hdfs dfs -du {os.path.dirname(hdfsPath)} | grep -e "{hdfsPath}"', shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).strip().split()[0]
                allLogs.append({'configFile': configFile, 'delmat': hdfsPath, 'type': 'output', 'feedAuthor': author, 'feedFailMail': failMail, 'feedDirection': direction, 'feedSystem': feedSystem, 'feedName': feedName, 'feedVersion': feedVersion})

    for toLog in debug:
        logger.log(toLog[0], toLog[1])
        
while not toDo.empty():
    main(toDo.get())

# Writes all missing delmat files to json file
with open(jsonFile, 'w', encoding='utf-8') as f:
    json.dump(allLogs, f, indent=4, ensure_ascii=False)

# Copy json file to HDFS if hdfs path is set
if pathHdfs:
    logger.info(f'Copying {jsonFile} to HDFS')
    subprocess.run(['hdfs', 'dfs', '-put', jsonFile, pathHdfs], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logger.info(f'Copied {jsonFile} to HDFS')

logger.info('DONE!')
