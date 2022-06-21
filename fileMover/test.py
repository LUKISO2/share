import logging, sys, os

class MaxLevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno < self.level

MIN_LEVEL = logging.DEBUG

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(MIN_LEVEL)

formater = logging.Formatter('%(asctime)s %(levelname)s; %(message)s')

outHandler = logging.StreamHandler(sys.stdout)
outHandler.setFormatter(formater)

lowerThanError = MaxLevelFilter(logging.ERROR)
outHandler.addFilter(lowerThanError)
outHandler.setLevel( MIN_LEVEL )

errHandler = logging.StreamHandler(sys.stderr)
errHandler.setLevel(logging.ERROR)
errHandler.setFormatter(formater)

errHandler.setLevel(max(MIN_LEVEL, logging.ERROR))

logger.addHandler(outHandler)
logger.addHandler(errHandler)

logger.debug("A DEBUG message")
logger.info("A INFO message")
logger.warning("A WARNING message")
logger.error("A ERROR message")
logger.critical("A CRITICAL message")