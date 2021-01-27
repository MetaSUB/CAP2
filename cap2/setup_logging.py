import logging
import luigi

LEVEL = 10


def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
    wrapper.has_run = False
    return wrapper


@run_once
def setup_logging():
    luigi.configuration.get_config().set('core', 'no_configure_logging', 'no_configure_logging')
    
    luigi_logger = logging.getLogger('luigi-interface')
    luigi_logger.setLevel(LEVEL)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LEVEL)

    formatter = logging.Formatter('[%(levelname)s] <luigi> (%(asctime)s): %(message)s')
    stream_handler.setFormatter(formatter)

    luigi_logger.handlers = []  # get rid of anything in luigi
    luigi_logger.addHandler(stream_handler)


    cap2_logger = logging.getLogger('cap2')
    cap2_logger.setLevel(LEVEL)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LEVEL)

    formatter = logging.Formatter('[%(levelname)s] <cap2> (%(asctime)s): %(message)s')
    stream_handler.setFormatter(formatter)

    cap2_logger.addHandler(stream_handler)


setup_logging()
