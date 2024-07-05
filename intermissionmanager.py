#!/usr/bin/env python3
import argparse, sys

from lib.intermission import Intermission
from lib.common import Logger, add_logger_args, get_logger_from_args

if __name__ == "__main__":
	parser = argparse.ArgumentParser()

	parser.add_argument(
		"--create-future-intermissions",
		help="Create intermission videos for all future occurances",
		action="store_true"
	)
	parser.add_argument(
		"--regenerate-existing",
		help="Recreate videos if they already exist",
		action="store_true"
	)
	parser.add_argument(
		"--cleanup-old",
		help="Clean up old intermission videos",
		action="store_true"
	)

	add_logger_args(parser)

	args = parser.parse_args()
	logger = get_logger_from_args(args)
	_print = logger._print

	intermission = Intermission(logger=logger)

	did_something = False

	if args.create_future_intermissions:
		_print("Creating future intermissions...")
		if args.regenerate_existing:
			intermission.generate_all_future_intermissions(regenerate_existing = True)
		else:
			intermission.generate_all_future_intermissions()
		_print("Done!")
		did_something = True

	if args.cleanup_old:
		_print("Cleaning up old intermissions...")
		intermission.delete_old_intermissions()
		_print("Done!")
		did_something = True

	if not did_something:
		print("Nothing to do!")
		print(f"Use {sys.argv[0]} --help")

	#intermission.generate_intermission_video(12435)
	#print(intermission.generate_voiceover_text(12166))
	#intermission._generate_speech_pyt2s(12166)
	#intermission._generate_audio_track(11980)
	#intermission.generate_all_future_intermissions()
