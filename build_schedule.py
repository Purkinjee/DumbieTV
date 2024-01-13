#!/usr/bin/env python3

from lib.scheduler import Scheduler
from datetime import datetime, timedelta

s = Scheduler()
s.build_schedule(datetime.now().date() - timedelta(days=1))
s.close()