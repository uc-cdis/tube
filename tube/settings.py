import os

from cdislogging import get_logger

logger = get_logger(__name__)
try:
    # Import everything from ``local_settings``, if it exists.
    from local_settings import *
except ImportError:
    # If it doesn't, look in ``/var/www/tube``.
    try:
        import imp
        imp.load_source('local_settings', '/var/www/tube/local_settings.py')
        print('finished importing')
    except IOError:
        logger.warn("local_settings is not found")
