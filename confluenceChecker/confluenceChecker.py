import configparser
import requests
import urllib3
import logging
import shutil
import bs4
import sys
import csv
import os

# Basic usage: python3 confluenceChecker.py <config_file>
# Requires the default aps.env enviroment

# Config file example, reffer to "Read configuration" section for more info:
# [TEST]
# debug_level = DEBUG
# path_to_configs = C:/Users/xxx/xxx/config-main.zip
# output_csv = C:/Users/xxx/xxx/output.csv
# temp_folder_location =
# username = xxx
# password = xxx

# Disable warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Hand changable variables
version = '1.1.0'

#temporaly logging before config
formater = logging.Formatter('%(asctime)s %(name)s$ [%(thread)d] %(levelname)s %(message)s')
tmpLog = logging.getLogger('Pre_config_log_of___%s___' % os.path.basename(__file__))
tmpLogHnd = logging.StreamHandler(sys.stderr)
tmpLogHnd.setLevel(logging.ERROR)
tmpLogHnd.setFormatter(formater)
tmpLog.addHandler(tmpLogHnd)

# Get environment variables
if len(sys.argv) < 2:
    tmpLog.error('No configuration file specified!')
    sys.exit(2)
config_file = sys.argv[1]
if config_file == '-h' or config_file == '--help':
    tmpLog.info('Usage: python3 confluenceChecker.py <config_file>')
    sys.exit(0)
confRoot = os.getenv('CONF_ROOT', '')
apsEnv = os.getenv('APS_ENV', 'TEST')

if confRoot is None:
    tmpLog.error('Environment variable CONF_ROOT is not set')
    sys.exit(2)

# Read configuration
if os.path.isfile(os.path.join(confRoot, config_file)):
    config = configparser.ConfigParser()
    config.read(os.path.join(confRoot, config_file))
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else 'DEBUG' # What level of messages will get logged (DEBUG-CRITICAL)
    zipFileRaw = config.get(apsEnv, 'path_to_configs') if 'path_to_configs' in config.options(apsEnv) else None # Exact path to archive or folder containing configs
    outCsv = config.get(apsEnv, 'output_csv') if 'output_csv' in config.options(apsEnv) else None # Output path including xxx.csv into which the output will be written
    tmpPath = config.get(apsEnv, 'temp_folder_location') if 'temp_folder_location' in config.options(apsEnv) else None # Path whete temporary files should be written, can be left empty for cwd
    username = config.get(apsEnv, 'username') if 'username' in config.options(apsEnv) else None # Intranet username (Used to authenticate to get results)
    password = config.get(apsEnv, 'password') if 'password' in config.options(apsEnv) else None # Intranet password (Used to authenticate to get results)
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

# Automatic mail reporting requirement
logger.info(f'Application: confluenceChecker.py, Version: {version}, Build: Unknown')

if not zipFileRaw:
    logger.error('Path to archive is not set')
    sys.exit(2)
else:
    zipPath = os.path.normpath(os.path.expandvars(zipFileRaw))
    zipFile = os.path.basename(zipPath)

skipUnpack = False
if not os.path.isfile(zipPath):
    logger.error('Archive not found, checking for existance')
    if not os.path.exists(zipPath):
        logger.error('Path does not exist')
        sys.exit(2)

    logger.info('Path exists, assuming unpacked folder')
    unpackFolder = zipPath
    skipUnpack = True

if not username:
    logger.error('Username is not set')
    sys.exit(2)

if not password:
    logger.error('Password is not set')
    sys.exit(2)

if not outCsv:
    logger.error('Output csv is not set')
    sys.exit(2)

if tmpPath == '':
    tmpPath = os.getcwd()
else:
    tmpPath = os.path.normpath(tmpPath)

# Tmp folder operations
def removeTree(tree):
    try:
        logger.debug(f'Removing recursively folder: {tree}')
        shutil.rmtree(tree)
    except Exception as e:
        logger.error(f'Error while removing folder: {e}')

def createFolder(folder):
    logger.debug(f'Creating folder: {folder}')
    if os.path.isdir(folder):
        removeTree(folder)
    os.mkdir(folder)

# main loop in try except block to ensure cleanup
try:
    temps = os.path.join(tmpPath, 'temp')
    createFolder(temps)

    if not skipUnpack:
        tmpFolderName = f'.tmp_{"".join(zipFile.split(".")[:-1])}'
        tmpFolder = os.path.join(temps, tmpFolderName)
        createFolder(tmpFolder)

        # Unpacking archive containing configs
        try:
            shutil.unpack_archive(zipPath, tmpFolder)
        except Exception as e:
            logger.error(f'Error while unpacking archive: {e}')
            sys.exit(2)

        unpackFolder = os.path.join(tmpFolder, 'config-main')

    configFolderName = f'tmp_configs'
    configFolder = os.path.join(temps, configFolderName)
    createFolder(configFolder)
    # Filtering out configs to separate folder
    for root, dirs, files in os.walk(unpackFolder):
        for file in files:
            if file.endswith('.conf'):
                if os.path.isfile(os.path.join(configFolder, file)):
                    shutil.copy(os.path.join(root, file), os.path.join(configFolder, f'{file}___{"".join(os.urandom(4).hex().split(" "))}.conf'))
                else:
                    shutil.copy(os.path.join(root, file), configFolder)

    # Reading configs
    class Config:
        def __init__(self, configPath: str):
            self.feedSystem = None
            self.feedName = None
            self.feedVersion = None
            self.feedDirection = None
            self.feedAuthor = None
            self.feedEmail = None
            self.kafkaTopics = []
            self.configPath = os.path.normpath(configPath)
            self.fileName = os.path.basename(self.configPath)

            self.loadConfig()

        def loadConfig(self):
            # Parses config file and sets class variables
            for line in open(self.configPath, 'r'):
                line = line.strip()
                if line.startswith('#'):
                    continue
                if line.startswith('feed_System'):
                    self.feedSystem = line.split('=')[1].strip().replace('"', '')
                    continue
                if line.startswith('feed_Name'):
                    self.feedName = line.split('=')[1].strip().replace('"', '')
                    continue
                if line.startswith('feed_Version'):
                    self.feedVersion = line.split('=')[1].strip().replace('"', '')
                    continue
                if line.startswith('feed_Direction'):
                    self.feedDirection = line.split('=')[1].strip().replace('"', '')
                    continue
                if line.startswith('feed_Author'):
                    self.feedAuthor = line.split('=')[1].strip().replace('"', '')
                    continue
                if line.startswith('feed_FailMail'):
                    self.feedEmail = line.split('=')[1].strip().replace('"', '')
                    continue
                if line.startswith('output_Kafka_Topic'):
                    splitted = line.split('=')
                    if len(splitted) > 1:
                        topic = splitted[1].strip().replace('"', '')
                        topic = topic.replace(r'${feed_Name}', (self.feedName if self.feedName else ''))
                        topic = topic.replace(r'${feed_System}', (self.feedSystem if self.feedSystem else ''))
                        topic = topic.replace(r'${feed_Version}', (self.feedVersion if self.feedVersion else ''))
                        topic = topic.replace(r'${feed_Direction}', (self.feedDirection if self.feedDirection else ''))
                        topic = topic.replace(r'${feed_Author}', (self.feedAuthor if self.feedAuthor else ''))
                        topic = topic.replace(r'${feed_FailMail}', (self.feedEmail if self.feedEmail else ''))
                        self.kafkaTopics.append(topic)

        def hasKafka(self):
            # Returns True if config has at least one kafka topic
            return len(self.kafkaTopics) > 0

        def contains(self, missing: list):
            # Returns True if config has at least one topic which are missing
            return not not set(self.kafkaTopics).intersection(missing)
        
        def missingTopics(self, missing: list):
            # Returns list of topics which are missing
            rmissing = []
            if not isinstance(missing, list):
                missing = [missing]
            for topic in self.kafkaTopics:
                if topic in missing:
                    rmissing.append(topic)
            return rmissing

        def delete(self):
            # Deletes config file
            os.remove(self.configPath)

    # Creating Response HTMLs
    class HTML:
        def __init__(self, topic: str, topicFolder: str):
            self.topic = topic
            self.topicFolder = topicFolder
            self.htmlPath = os.path.join(topicFolder, f'{topic}.html')

        def request(self, session: requests.Session):
            # Requests HTML with search results from Confluence
            p = session.get(f'https://vpconfluence.cz.tmo/dosearchsite.action?queryString={self.topic}', verify=False, allow_redirects=True)
            p.encoding = p.apparent_encoding
            with open(self.htmlPath, 'w', encoding='utf-8') as f:
                f.write(p.text)

        def hasDocumentation(self):
            # Returns True if HTML contains at least one search result
            html = open(self.htmlPath, 'r', encoding='utf-8').read()
            soup = bs4.BeautifulSoup(html, 'html.parser')
            allResults = soup.find('ol', {'class': 'search-results'})
            if allResults is None:
                return False
            return True

    allConfigs = []
    allHTMLs = []

    # Searching for unique topics
    topics = set()
    for file in os.listdir(configFolder):
        logger.debug(f'Parsing config file: {file}')
        config = Config(os.path.join(configFolder, file))
        allConfigs.append(config)
        if config.hasKafka():
            for topic in config.kafkaTopics:
                logger.debug(f'Found topic: {topic}')
                topics.add(topic)

    topicFolderName = 'topics_htmls'
    topicFolder = os.path.join(temps, topicFolderName)
    createFolder(topicFolder)

    # Html payload
    data = {
        'IDToken1': username,
        'IDToken2': password,
        'IDButton': 'Sign+In',
        'goto': r'aHR0cHM6Ly92cGNvbmZsdWVuY2UuY3oudG1vL3NlYXJjaC9zZWFyY2h2My5hY3Rpb24%3D',
        'gotoOnFail': '',
        'SunQueryParamsString': 'bW9kdWxlPW9uZUxEQVAmcmVhbG09L2VtcGxveWVl',
        'encoded': True,
        'gx_charset': 'UTF-8'
    }

    # Creating session and logging in to EAM
    logger.debug('Creating session and loging in to EAM')
    session = requests.Session()
    session.post('https://eam.cz.tmo/eam/UI/Login', data=data, verify=False, allow_redirects=True)

    # Getting HTMLs for all unique topics
    for topic in topics:
        logger.debug(f'Getting HTML for topic: {topic}')
        html = HTML(topic, topicFolder)
        allHTMLs.append(html)
        html.request(session)

    # Checking if all topics have documentation
    logger.debug('Checking if all topics have documentation')
    missing = []
    for html in allHTMLs:
        if not html.hasDocumentation():
            logger.debug(f'Topic {html.topic} is missing documentation')
            missing.append(html.topic)

    header = ['topic', 'configName', 'feedName', 'feedSystem', 'feedVersion', 'feedDirection', 'feedAuthor', 'feedEmail']
    csvData = []

    # Checking if configs does not contain topics without documentation
    logger.debug('Checking if confings does not contain topics without documentation')
    for config in allConfigs:
        if config.contains(missing):
            for topic in config.missingTopics(missing):
                csvData.append([topic, config.fileName, config.feedName, config.feedSystem, config.feedVersion, config.feedDirection, config.feedAuthor, config.feedEmail])
            logger.debug(f'Config {config.fileName} is missing documentation for topics: {", ".join(config.missingTopics(missing))}')

    # Creating CSV file
    logger.debug('Creating CSV file')
    with open('missing.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(csvData)

except Exception as e:
    logger.error(f'An error occured, deleting temp files:\n{e}')

finally:
    logger.debug('Deleting configs')
    removeTree(temps)
