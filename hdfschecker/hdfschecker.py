#!/opt/cloudera/parcels/Anaconda/envs/python36/bin/python
import logging.handlers
import configparser
import pyhocon
import logging
import pandas
import sys
import re
import os

# Hand changable variables
version = '1.2.3'
configs = []
csv = []

#temporaly logging before config
formater = logging.Formatter('%(asctime)s %(name)s$ [%(thread)d] %(levelname)s %(message)s')
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
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else 'debug'
    logFile = config.get(apsEnv, 'log_file') if 'log_file' in config.options(apsEnv) else None
    csvFile = config.get(apsEnv, 'csv_output') if 'csv_output' in config.options(apsEnv) else None
else:
    tmpLog.error('Configuration file not found')
    sys.exit(2)

if logFile is None:
    tmpLog.error('No log_file name specified, using default: app.log')
    logFile = 'app.log'

if csvFile is None:
    tmpLog.error('No csv_output file specified, using default: app.csv')
    sys.exit(2)

if not os.path.isdir(os.path.dirname(os.path.normpath(csvFile))):
    tmpLog.error('CSV output directory does not exist')
    sys.exit(2)

if not re.search('.csv$', csvFile):
    tmpLog.error('CSV output file must have .csv extension, using default filename: app.csv')
    csvFile = os.path.join(os.path.dirname(os.path.normpath(csvFile)), 'app.csv')

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

# Sets basic and missing env variables
os.environ['CONF_DIR'] = os.environ['CONF_ROOT']
logger.debug(f'LOADED: CONF_DIR={os.environ["CONF_DIR"]}')
os.environ['COMMONCONF_DIR'] = f'{os.environ["CONF_ROOT"]}/common'
logger.debug(f'LOADED: COMMONCONF_DIR={os.environ["COMMONCONF_DIR"]}')
os.environ['SQL_DIR'] = f'{os.environ["CONF_ROOT"]}/sql'
logger.debug(f'LOADED: SQL_DIR={os.environ["SQL_DIR"]}')
os.environ['COMMONSQL_DIR'] = f'{os.environ["SQL_DIR"]}/common'
logger.debug(f'LOADED: COMMONSQL_DIR={os.environ["COMMONSQL_DIR"]}')

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

def check(checki):
    for item in checki:
        if item[0] == 'config_File':
            logger.debug(f'Found another config file contained in a config file: {item[1]}')
            check(loadEnviron(item[1]))
check(loadEnviron(os.path.join(os.environ['COMMONCONF_DIR'], f'general_{os.environ["DATAINGEST_ENV"]}.conf')))

# Data processing
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
            configs.append(configFile)

def main(configFile, logger=logger, csv=csv):
    # Loads required variables into env to be used by parser and logging
    something = False
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

    # Loads input from files and checks for LocalFS input_Type
    inputType = config.get('input_Type', None)

    if inputType == 'LocalFS':
        inputPath = config.get('input.input_Path', None)
        if inputPath is not None:
            delmatToCheckPath = os.path.normpath(os.path.expandvars(inputPath).replace('"', '').replace('\\', '/').replace('//', '/').split('/p_')[0].replace(r'${feed_System}', feedSystem).replace(r'${feed_Name}', feedName).replace(r'${feed_Version}', feedVersion))
            debug.append([logging.INFO, f'Found input LocalFS path, adding to csv: {delmatToCheckPath}'])
            csv.append([os.path.basename(os.path.normpath(configFile)), 'input', feedSystem, feedName, feedVersion, author, 'LocalFS', delmatToCheckPath])
            something = True

    if inputType == 'JDBC':
        inputPath = config.get('input.input_JDBC_URL', None)
        if inputPath is not None:
            delmatToCheckPath = os.path.normpath(os.path.expandvars(inputPath).replace('"', '').replace('\\', '/').replace('//', '/').split('/p_')[0].replace(r'${feed_System}', feedSystem).replace(r'${feed_Name}', feedName).replace(r'${feed_Version}', feedVersion))
            debug.append([logging.INFO, f'Found input JDBC path, adding to csv: {delmatToCheckPath}'])
            csv.append([os.path.basename(os.path.normpath(configFile)), 'input', feedSystem, feedName, feedVersion, author, 'JDBC', delmatToCheckPath])
            something = True

    # Does basically the same as input, but checkes ALL outputs for HDFS and raises error if path not found in DELMAT
    outputs = config.get('output', [])
    for output in outputs:
        outputType = output.get('output_Type', None)

        if outputType == 'LocalFS':
            outputHDFS = output.get('output_Path', None)
            if outputHDFS is not None:
                outputPath = os.path.normpath(os.path.expandvars(outputHDFS).replace('"', '').replace('\\', '/').replace('//', '/').replace(r'${feed_System}', feedSystem).replace(r'${feed_Name}', feedName).replace(r'${feed_Version}', feedVersion))
                debug.append([logging.INFO, f'Found output LocalFS path, adding to csv: {outputPath}'])
                csv.append([os.path.basename(os.path.normpath(configFile)), 'output', feedSystem, feedName, feedVersion, author, 'LocalFS', outputPath])
                something = True

        if outputType == 'JDBC':
            outputPath = output.get('output_JDBC_URL', None)
            if outputPath is not None:
                outputPath = os.path.normpath(os.path.expandvars(outputPath).replace('"', '').replace('\\', '/').replace('//', '/').split('/p_')[0].replace(r'${feed_System}', feedSystem).replace(r'${feed_Name}', feedName).replace(r'${feed_Version}', feedVersion))
                debug.append([logging.INFO, f'Found output JDBC path, adding to csv: {outputPath}'])
                csv.append([os.path.basename(os.path.normpath(configFile)), 'output', feedSystem, feedName, feedVersion, author, 'JDBC', outputPath])
                something = True

    if not something:
        debug.append([logging.INFO, 'No input or output found in config file: ' + configFile])
        csv.append([os.path.basename(os.path.normpath(configFile)), 'both', feedSystem, feedName, feedVersion, author, 'None', 'None'])

    for toLog in debug:
        logger.log(toLog[0], toLog[1])
    
for item in configs:
    main(item)

pandas.DataFrame(csv, columns=['config name', 'direction', 'feed_System', 'feed_Name', 'feed_Version', 'feed_Author', 'type', 'path']).to_csv(csvFile, index=False)

logger.info('DONE!')
