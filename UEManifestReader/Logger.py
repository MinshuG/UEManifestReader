import logging

logger = logging.getLogger("ManifestDownloader")
create_file = False


def get_logger(logger_name: str):
    logger_name = logger_name.split(".")[-1]

    log = logger.getChild(logger_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if create_file:
        fh = logging.FileHandler('UE4Parse.log')
        fh.setLevel(level=logging.INFO)
        fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(level=logging.INFO)
    ch.setFormatter(formatter)

    if create_file:
        log.addHandler(fh)

    log.addHandler(ch)
    return log
