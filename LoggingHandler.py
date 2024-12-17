import logging
import os

class Handler:
    def __init__(self):
        # Configure error the logger
        error_log_path = os.path.expanduser('~')+'/shop_errors.log'
        error_logger = logging.getLogger('error_logger')
        error_logger.setLevel(logging.DEBUG)  # Set the logging level
        error_file_handler = logging.FileHandler(error_log_path, mode='a')
        error_file_handler.setLevel(logging.DEBUG)
        error_logger.addHandler(error_file_handler)

        # Configure the logger
        info_log_path = os.path.expanduser('~')+'/shop_info.log'
        info_logger = logging.getLogger('info_logger')
        info_logger.setLevel(logging.DEBUG)  # Set the logging level
        info_file_handler = logging.FileHandler(info_log_path, mode='a')
        info_file_handler.setLevel(logging.DEBUG)
        info_logger.addHandler(info_file_handler)
        
        return error_logger,info_logger