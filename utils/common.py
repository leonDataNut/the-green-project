import logging
import sys
import time
import threading
import os
from dotenv import load_dotenv

# Loading Local env variables
load_dotenv()

#########################################
# Logging
#########################################


def setup_logging(module=None, level=logging.INFO):  # pragma: no cover
    """Configures the given logger (or the root logger) to output to the supplied
    stream (or standard out) at the supplied logging level (or INFO).  Also
    configures all additional loggers."""
    logger = logging.getLogger(module or '')
    logger.setLevel(level)
    logging.Formatter.converter = time.gmtime
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(processName)s - %(levelname)s - %(message)s'
    )
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


class progress_percentage(object):
    def __init__(self, progress_name, total_size):
        self.progress_name = progress_name
        self._size = total_size
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self.start_time = time.time()


    def __call__(self, chunks:int=1, msg=None):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += chunks
            percentage = (self._seen_so_far / self._size) * 100 if self._size else 0
            time_diff = time.time() - self.start_time
            time_diff_msg = "{} mins elapsed".format(round(time_diff/60,1)) if time_diff>60 else "{} secs elapsed".format(round(time_diff,1))
            msg = "\r%s  %s / %s  (%.2f%%) %s %s" % (
                    self.progress_name, self._seen_so_far, self._size,
                    percentage, msg or '',time_diff_msg)

            sys.stderr.write(msg)

            if self._seen_so_far == self._size and self.is_operational_os:
                sys.stderr.write('\n')

            sys.stderr.flush()


#########################################
# DB Constants
#########################################         

IMPORT_SCHEMA = os.environ["DB_IMPORT_SCHEMA"]
STAGING_SCHEMA = os.environ["DB_REPORTING_SCHEMA"]
REPORTING_SCHEMA = os.environ["DB_STAGING_SCHEMA"]
