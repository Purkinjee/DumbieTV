#!/usr/bin/env python3
import argparse

from lib.player import Player
from lib.common import Logger, add_logger_args, get_logger_from_args

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	add_logger_args(parser)

	args = parser.parse_args()
	logger = get_logger_from_args(args)
	_print = logger._print

	p = Player(logger=logger)
	p.play()
	p.close()