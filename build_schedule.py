#!/usr/bin/env python3
import argparse, sys
from datetime import datetime, timedelta

from lib.scheduler import Scheduler
import config
from lib.vars import *
from lib.common import Logger, add_logger_args, get_logger_from_args

if __name__ == "__main__":
	parser = argparse.ArgumentParser()

	parser.add_argument(
		"--create-schedule",
		help="Create schedule for specified date if given",
		action="store_true"
	)
	parser.add_argument(
		"--date",
		help="Date for which to build the schedule. (YYYYMMDD)",
		type=str
	)
	parser.add_argument(
		"--xmltv",
		help="Build xmltv file when done with scheduling",
		action="store_true"
	)
	parser.add_argument(
		"--adjust-times",
		help="Adjust future listings based off actual end times of shows",
		action="store_true"
	)
	add_logger_args(parser)

	args = parser.parse_args()
	logger = get_logger_from_args(args)
	_print = logger._print

	scheduler = Scheduler(logger=logger)
	
	did_something = False
	if args.adjust_times:
		_print("Adjusting times for future schedule items...")
		scheduler.adjust_schedule_times()
		_print("Done!")
		did_something = True
	
	if args.create_schedule:
		if args.date is not None:
			try:
				date = datetime.strptime(args.date, "%Y%m%d").date()
			except:
				print("Unable to parse date. Use YYYYMMDD")
				sys.exit()
			
			_print(f"Creating schedule for {args.date}...")
			scheduler.build_schedule(date=date)
		
		else:
			_print("Creating schedule...")
			scheduler.build_schedule()
		_print("Done!")
		did_something = True

	if args.xmltv:
		_print("Generating XMLTV...")
		scheduler.generate_xmltv(config.XMLTV_LOCATION)
		_print("Done!")
		did_something = True

	if not did_something:
		print("Nothing to do!")
		print(f"Use {sys.argv[0]} --help")

	scheduler.close()