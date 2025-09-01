import logging

def create_logger(name: str, filename: str):
    """
    Создаёт логгер с указанным именем и файлом.
    Формат строки: CSV
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(filename, mode='w', encoding="utf-8")
        fh.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(fh)
    return logger

def log_line(logger: logging.Logger, line: str):
    logger.info(line)

