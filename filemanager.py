#!/usr/bin/env python3
import argparse, sys

from lib.meta import TVScanner
from lib.common import _print

if __name__ == "__main__":
	scanner = TVScanner()
	parser = argparse.ArgumentParser()

	parser.add_argument(
		"--add-new-shows", 
		help="Add shows to the databse that have not yet been added",
		action="store_true"	
	)
	parser.add_argument(
		"--add-new-episodes",
		help="Add episodes to verified shows that have not yet been added",
		action="store_true"
	)
	parser.add_argument(
		"--purge-missing-episodes",
		help="Purge episodes from databse that don't exist on disk",
		action="store_true"
	)
	parser.add_argument(
		"--update-shows",
		help="Update show metadata that need to be updated or are stale",
		action="store_true"
	)
	parser.add_argument(
		"--show-id",
		help="Force update of SHOW_ID. Only useful with --update-shows",
		type=int
	)
	parser.add_argument(
		"--cleanup-last-played",
		help="Make sure last played episode is correct. Useful if the schedule has been manually edited",
		action="store_true"
	)

	args = parser.parse_args()

	did_something = False
	if args.add_new_shows:
		_print("Adding new shows...")
		scanner.add_new_shows()
		_print("Done!")
		did_something = True

	if args.add_new_episodes:
		_print("Adding new episodes...")
		scanner.add_new_episodes()
		_print("Done!")
		did_something = True

	if args.update_shows:
		if args.show_id is None:
			_print("Updating shows...")
		else:
			_print(f"Updating show {args.show_id}")
		
		scanner.update_shows(show_id = args.show_id)
		_print("Done!")
		did_something = True

	if args.purge_missing_episodes:
		_print("Purging missing episodes...")
		scanner.remove_missing_episodes()
		_print('Done!')
		did_something = True

	if args.cleanup_last_played:
		_print("Cleaning up last played episodes...")
		scanner.cleanup_last_played_episodes()
		_print('Done!')
		did_something = True
	
	if not did_something:
		print("Nothing to do!")
		print(f"Use {sys.argv[0]} --help")
	
	scanner.close()
	

	
