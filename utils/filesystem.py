import json
from utils.common import setup_logging
from datetime import datetime


TEMP_DIR = "/tmp"
LOG = setup_logging(__name__)


def generate_tmp_file_name(extention=""):
    name = int(datetime.now().timestamp())
    return f"{name}{extention}"


class writers:
    log_template = "Creating file {file_path}"


    def dict_to_file(dict_obj, file_name=None, folder=TEMP_DIR, verbose=True):
        file_name = file_name if file_name else generate_tmp_file_name(".json")
        file_path = f"{folder}/{file_name}"
        
        if verbose:
            LOG.info(writers.log_template.format(file_path=file_path) )

        with open(file_path, "w") as f:
            json.dump(dict_obj,f)

        return file_path