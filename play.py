#!/usr/bin/env python3
import argparse

from lib.player import Player
from lib.common import Logger, add_logger_args, get_logger_from_args

if __name__ == "__main__":
	parser = argparse.ArgumentParser()

	parser.add_argument(
		"--force-schedule-id",
		help="Only play file for provided schedule id. Used for testing purposes only.",
		type=int
	)
	parser.add_argument(
		"--verbose",
		help="Show all ffmpeg output",
		action="store_true"
	)
	add_logger_args(parser)

	args = parser.parse_args()
	logger = get_logger_from_args(args)
	_print = logger._print

	p = Player(logger=logger)
	p.play(
		force_schedule_id = args.force_schedule_id,
		verbose = args.verbose
	)
	p.close()
