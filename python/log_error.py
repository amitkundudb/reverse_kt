import logging
logging.basicConfig(filename='error.log',level = logging.ERROR)

try:
    result = 10 / 0
except ZeroDivisionError as e:
    logging.error(f"Error: {e}", exc_info=True)