import mysql.connector
import requests
from PIL import Image
from io import BytesIO

import config

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