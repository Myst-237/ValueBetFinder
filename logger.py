import logging
from logging.handlers import RotatingFileHandler
import socket
import sys
LOG_PATH = 'logs.log'

class WebScraperLogger:
    """
    A logger class to handle logging for web scraping activities. 

    Attributes
    ----------
    - logger: logging.Logger
        A logging object which is responsible for logging messages.

    Parameter
    -------
    - name: name of the logger
    - level= desired log level. default = logging.DEBUG, 
    - log_file_path: where to store the logs, default = config.settings.LOG_PATH
    
    """
    def __init__(self, name: str, level=logging.DEBUG, log_file_path=LOG_PATH):
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # create rotating file handler to store logs in file
        file_handler = RotatingFileHandler(log_file_path, maxBytes=1000000, backupCount=1)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
        # create socket handler to send logs to logging software
        # try:
        #     logging_server = 'logs2.papertrailapp.com'
        #     logging_port = 37343
        #     socket_handler = logging.handlers.SysLogHandler(address=(logging_server, logging_port))
        #     #socket_handler = logging.handlers.SocketHandler(logging_server, logging_port)
        #     socket_handler.setLevel(logging.DEBUG)
        #     socket_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        #     self.logger.addHandler(socket_handler)
        # except socket.error:
        #     pass
        
    def debug(self, message):
            self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)
        
    def exception(self, message):
        self.logger.exception(message)
        
    def set_level(self, level):
        self.logger.setLevel(level)
        
    def add_file_handler(self, filename, max_bytes=1048576, backup_count=5, level=logging.DEBUG):
        file_handler = RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=backup_count)
        file_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
    def remove_file_handler(self):
        for handler in self.logger.handlers:
            if isinstance(handler, RotatingFileHandler):
                self.logger.removeHandler(handler)

            
    def disable(self):
        logging.disable(sys.maxsize)
