#!/opt/freeware/bin/python2.6
############################################################################################################################
#
# Move files from input directory to the tape backup directory and prepare control file for backup engine
# 1. Move files (by the file mask) to output directory (tape backup directory)
# 2. Create list file with list of backuped files ("listing.txt")
# 3. Add list of backuped files to the full list of backuped files
# Use:
# move_for_backup.py <configuration_file>
#

import ConfigParser
import datetime
#import argparse
import logging
import shutil
import sys
import re
import os

#Basic Values
movedFiles = []
matched = 0
rndNum = str(os.getpid()) + '_' + str(datetime.datetime.now().microsecond / 1000)

#temporaly logging before config
tmpLog = logging.getLogger('Pre_config_log_of___%s___' % os.path.basename(__file__))
tmpLogHnd = logging.StreamHandler(sys.stderr)
tmpLogHnd.setLevel(logging.ERROR)
tmpLogHnd.setFormatter(logging.Formatter('%(asctime)s %(levelname)s; %(message)s'))
tmpLog.addHandler(tmpLogHnd)

#getting file name argument
#argparser = argparse.ArgumentParser(description='Moves files with excesive logging')
#argparser.add_argument('config', type=str, help='Name of the config file', nargs=1)
#config_file = argparser.parse_args().config[0]
config_file = sys.argv[1]

# Get environment variables
cwd = os.getcwd()
confRoot = os.path.join(os.getenv('APL_DIR'),'config')
apsEnv = os.getenv('APS_ENV','PROD')

if confRoot is None:
    tmpLog.error('Environment variable CONF_ROOT is not set')
    sys.exit(2)

# Read configuration
if os.path.isfile(os.path.join(confRoot, config_file)):
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(confRoot, config_file))
    inputDir = config.get(apsEnv, 'input_dir') if 'input_dir' in config.options(apsEnv) else None
    outputDir = config.get(apsEnv, 'output_dir') if 'output_dir' in config.options(apsEnv) else None
    listFile = config.get(apsEnv, 'list_file') if 'list_file' in config.options(apsEnv) else None
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
logger.debug('Debug level: %s' % debugLevel)
logger.debug('Input directory: %s' % inputDir)
logger.debug('Output directory: %s' % outputDir)
logger.debug('List file: %s' % listFile)
logger.debug('File mask: %s' % fileMask)
logger.debug('temporary file extension is: %s' % rndNum)

#Checks if the config file loaded correctly
if None in [inputDir, outputDir, listFile, fileMask, debugLevel]:
    logger.error("Missing configuration parameters")
    sys.exit(2)

#Checks existence of parameters
if not os.path.isdir(inputDir):
    logger.error("Input directory doesn't exist")
    sys.exit(2)
if not os.path.isdir(outputDir):
    logger.error("Output directory doesn't exist")
    sys.exit(2)
if not os.path.isfile(listFile):
    logger.error("'List file' doesn't exist")
    sys.exit(2)
if os.path.isfile(os.path.join(outputDir, 'listing.txt')):
    logger.error("'listing.txt' already exists - stopped")
    sys.exit(1)
try: re.compile(fileMask)
except re.error:
    logger.error("Invalid file mask")
    sys.exit(2)

#starts moving files
logger.info('Starting moving files')
logger.debug('----------')
for file in sorted(os.listdir(inputDir)):
    logger.debug('Matching file...: %s' % file)
    if re.match(fileMask, file):
        logger.info('Matched found, moving: %s' % file)
        try: shutil.move(os.path.join(inputDir, file), os.path.join(outputDir, file))
        except Exception as e:
            logger.error('Failed to move file: "%s" because: %s' %(file, str(e)))
            sys.exit(2)
        logger.debug('Checking if file was moved...')
        if os.path.isfile(os.path.join(outputDir, file)):
            logger.debug('Passed quick check')
            if os.path.getsize(os.path.join(outputDir, file)) == os.path.getsize(os.path.join(inputDir, file)):
                logger.debug('Passed size check')
                logger.info('File moved: %s' % file)
            else:
                logger.error('Moved file is incompleate: %s' % file)
                sys.exit(2)
        else:
            logger.error('File not moved: %s' % file)
            sys.exit(2)
        movedFiles.append(file)
        logger.debug('Move successful')
        with open(listFile, 'a') as f:
            f.write(file + '\n')
        f.close()
        logger.debug('File successfully added to list')
        matched += 1
logger.debug('----------')

#checks if any files were moved
if matched == 0:
    logger.info("No files matched the mask")
    sys.exit(1)
logger.info('successfully moved %s files' % matched)

#copies list file for backup
logger.debug('Copying list file to be backuped')
try: shutil.copy(listFile, outputDir)
except:
    logger.error('Failed to copy list file to output directory')
    sys.exit(2)
logger.debug('List file successfully copied')

#creating listing.txt
tmpListing = os.path.join(outputDir, 'listing_tmp_%s.txt' % rndNum)
logger.debug('Creating temporary listing file: %s' % tmpListing)

with open(tmpListing, 'w') as f:
    for file in movedFiles:
        f.write(file + '\n')
    f.write(os.path.basename(listFile))
f.close()

logger.debug('Seccessfuly written temporary listing file')

try: shutil.move(tmpListing, os.path.join(outputDir, 'listing.txt'))
except Exception as e:
    logger.error('Failed to rename listing file because %s' % str(e))
    sys.exit(2)
finally:
    logger.debug('Seccessfuly created listing.txt file')
