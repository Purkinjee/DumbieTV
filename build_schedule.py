#!/usr/bin/env python3
import argparse, sys
from datetime import datetime, timedelta

from lib.scheduler import Scheduler
import config

if __name__ == "__main__":
	scheduler = Scheduler()
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

	args = parser.parse_args()
	
	did_something = False
	if args.adjust_times:
		print("Adjusting times for future schedule items...")
		scheduler.adjust_schedule_times()
		print("Done!")
		did_something = True
	
	if args.create_schedule:
		if args.date is not None:
			try:
				date = datetime.strptime(args.date, "%Y%m%d").date()
			except:
				print("Unable to parse date. Use YYYYMMDD")
				sys.exit()
			
			print(f"Creating schedule for {args.date}...")
			scheduler.build_schedule(date=date)
		
		else:
			print("Creating schedule...")
			scheduler.build_schedule()
		print("Done!")
		did_something = True

	if args.xmltv:
		print("Generating XMLTV...")
		scheduler.generate_xmltv(config.XMLTV_LOCATION)
		print("Done!")
		did_something = True

	if not did_something:
		print("Nothing to do!")
		print(f"Use {sys.argv[0]} --help")

	scheduler.close()