import logging
from os import path

def process_daily_transactions(file_location, date_part):
    logging.info(f'file_location: {file_location}')
    logging.info(f'date_part: {date_part}')

    base_filename = f'{date_part}_transactions'
    suffix = '.json'
    file_full_path = path.join(file_location, base_filename + suffix)
    logging.info(f'file_full_path: {file_full_path}')

