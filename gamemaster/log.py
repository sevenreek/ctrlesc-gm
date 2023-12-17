from logging import getLogger, StreamHandler

BASE_LOGGER = "base"
stdout_handler = StreamHandler()
log = getLogger(BASE_LOGGER)
log.addHandler(stdout_handler)

from settings import settings

log.setLevel(settings.log_level)
