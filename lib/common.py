import mysql.connector
import requests
from PIL import Image
from io import BytesIO
from datetime import datetime

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

def _print(message, level=LOG_LEVEL_INFO, file=None):
	if level > config.LOG_LEVEL:
		return
	level_str = {
		LOG_LEVEL_ERROR: "Error",
		LOG_LEVEL_INFO: "Info",
		LOG_LEVEL_DEBUG: "Debug"
	}
	if file is not None:
		pass
	else:
		now = datetime.now().strftime("%b %d %H:%M:%S")
		print(f"{now} [{level_str[level]}] {message}")
		