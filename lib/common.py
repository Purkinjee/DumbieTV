import mysql.connector
import requests
from PIL import Image
from io import BytesIO
from datetime import datetime
import os

import config
from lib.vars import *

def get_mysql_connection():
	return mysql.connector.connect(
		user = config.MYSQL_USER,
		password = config.MYSQL_PASSWORD,
		host = config.MYSQL_HOST,
		database = config.MYSQL_DB
	)

def get_image_dimensions(url):
	data = requests.get(url).content
	im = Image.open(BytesIO(data))    
	return im.size

def add_logger_args(parser):
	parser.add_argument(
		"--log-level",
		help="Log Level",
		choices=["error", "info", "debug"],
		type=str,
		default="info"
	)

	parser.add_argument(
		"--log-file",
		help="Log to given file",
		type=str
	)

	parser.add_argument(
		"--no-stdout",
		help="Do not output to stdout",
		action="store_true"
	)
def get_logger_from_args(args):
	stdout = True
	if args.no_stdout:
		stdout = False

	log_level = {
		"error": LOG_LEVEL_ERROR,
		"info": LOG_LEVEL_INFO,
		"debug": LOG_LEVEL_DEBUG
	}.get(args.log_level, LOG_LEVEL_INFO)
	
	return Logger(
		log_level = log_level,
		stdout = stdout,
		file = args.log_file
	)

class Logger:
	def __init__(self, log_level=LOG_LEVEL_INFO, stdout=True, file=None):
		self.log_level = log_level
		self.log_level = 100
		self.stdout = stdout
		self.log_file = file

	def _print(self, message, level=LOG_LEVEL_INFO, file=None):
		if level > self.log_level:
			return
		level_str = {
			LOG_LEVEL_ERROR: "Error",
			LOG_LEVEL_INFO: "Info",
			LOG_LEVEL_DEBUG: "Debug"
		}
		now = datetime.now().strftime("%b %d %H:%M:%S")
		messagef = f"{now} [{level_str[level]}] {message}"
		if self.log_file is not None:
			if not os.path.exists(self.log_file):
				head, tail = os.path.split(self.log_file)
				os.makedirs(head, exist_ok=True)
			
			with open(self.log_file, 'a') as f:
				f.write(f"{messagef}\n")

			
		if self.stdout:
			print(messagef)
		