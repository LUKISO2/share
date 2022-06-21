import configparser
import argparse
import logging
import shutil
import sys
import re
import os

#Basic Values
matched = 0

#temporaly logging before config
tmpLog = logging.getLogger(f'Pre_config_log_of___{os.path.basename(__file__)}___')
tmpLogHnd = logging.StreamHandler(sys.stderr)
tmpLogHnd.setLevel(logging.ERROR)
tmpLogHnd.setFormatter(logging.Formatter('%(asctime)s %(levelname)s; %(message)s'))
tmpLog.addHandler(tmpLogHnd)

#getting file name argument
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
    config = configparser.ConfigParser()
    config.read(os.path.join(confRoot, config_file))
    inputDir = config.get(apsEnv, 'input_dir') if 'input_dir' in config.options(apsEnv) else None
    fileMask = config.get(apsEnv, 'file_mask') if 'file_mask' in config.options(apsEnv) else None
    debugLevel = config.get(apsEnv, 'debug_level') if 'debug_level' in config.options(apsEnv) else None
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
logger.debug(f'Directory: {inputDir}')
logger.debug(f'File mask: {fileMask}')

#Checks if the config file loaded correctly
if None in [inputDir, fileMask, debugLevel]:
    logger.error("Missing configuration parameters")
    sys.exit(2)

#Checks existence of parameters
if not os.path.isdir(inputDir):
    logger.error("Input directory doesn't exist")
    sys.exit(2)
try: re.compile(fileMask)
except re.error:
    logger.error("Invalid file mask")
    sys.exit(2)

#starts renaming
for file in os.listdir(inputDir):
    logger.debug(f'Matching file...: {file}')
    if re.match(fileMask, file):
        logger.debug(f'Match found, renaming: {file}')
        renamed = file.split('.')[:-1] if file.split('.')[-1] == 'tmp' else file
        if renamed == file:
            logger.warning(f'File {file} matched the mask, but is not ending with .tmp, skipping')
            continue
        try: shutil.move(os.path.join(inputDir, file), os.path.join(inputDir, '.'.join(renamed)))
        except Exception as e:
            logger.error(f'Failed to move file: "{file}" because: {str(e)}')
            sys.exit(2)
        finally:
            logger.debug(f'Successfully renamed to: {".".join(renamed)}')
            matched += 1

#Checks if any files were renamed
if matched == 0:
    logger.error("No files matched the mask")
    sys.exit(1)
logger.info(f'Successfully renamed {matched} files')