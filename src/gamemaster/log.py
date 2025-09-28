from logging import getLogger, StreamHandler

from gamemaster.settings import settings

BASE_LOGGER = "base"
stdout_handler = StreamHandler()
log = getLogger(BASE_LOGGER)
log.addHandler(stdout_handler)


log.setLevel(settings.log_level)
