import logging
import datetime


def configure_logging():
    # Configure the logging system with a custom format
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] [%(filename)s:%(module)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


# Call the configure_logging function to set up the logging configuration
configure_logging()


# Optionally, define a function to get a logger instance in other files
def get_logger(name):
    return logging.getLogger(name)
