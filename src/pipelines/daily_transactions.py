import logging
from os import path

def process_daily_transactions(file_location: str, date_part: str) -> None:
    """
    Reads the file specified by file_location and date_part into DataFrame
    Calls TransactionSparkTransformer to transform the DataFrame
    Writes the DataFrame into MongoDB

    Parameters
    ----------
    file_location : str
        The file path
    date_part : str
        The date part in the file name

    Returns
    -------
    None
    """

    logging.info(f'file_location: {file_location}')
    logging.info(f'date_part: {date_part}')

    base_filename = f'{date_part}_transactions'
    suffix = '.json'
    file_full_path = path.join(file_location, base_filename + suffix)
    logging.info(f'file_full_path: {file_full_path}')

