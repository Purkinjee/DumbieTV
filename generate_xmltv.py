#!/usr/bin/env python3

from lib.scheduler import Scheduler
from datetime import datetime, timedelta
import config

s = Scheduler()
s.generate_xmltv(config.XMLTV_LOCATION)
s.close()