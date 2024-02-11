import os
import re
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

from imdb import Cinemagoer
import tvdb_v4_official

from lib.common import get_mysql_connection, get_image_dimensions, _print
from lib.vars import *
import config

class TVScanner:
	def __init__(self):
		self._db = get_mysql_connection()

	def close(self):
		self._db.close()

	def add_new_shows(self):
		cur = self._db.cursor()

		tv_root = Path(config.TV_SHOW_DIR)
		tv_show_dirs = [x for x in tv_root.iterdir() if x.is_dir()]

		tvdb = tvdb_v4_official.TVDB(config.TVDB_API_KEY)
		for show_dir in tv_show_dirs:
			q = "SELECT id FROM tv_shows WHERE path = %s"
			cur.execute(q, (str(show_dir), ))
			res = cur.fetchone()
			if res:
				_print(f"{str(show_dir)} exists in DB, skipping...", LOG_LEVEL_DEBUG)
				continue


			show_name = show_dir.parts[-1]
			res = tvdb.search(show_name)
			if not res:
				_print(f"Could not retrieve TVDB info for {show_name}", LOG_LEVEL_ERROR)
				continue
			res = res[0]

			thumbnail_width = 0
			thumbnail_height = 0
			if res.get('thumbnail', None):
				thumbnail_width, thumbnail_height = get_image_dimensions(res['thumbnail'])

			q = (
				"INSERT INTO tv_shows "
				"(tvdb_id, path, title, description, thumbnail, "
				"thumbnail_width, thumbnail_height, verified, last_updated) "
				"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
			)
			cur.execute(q, (
				res['tvdb_id'],
				str(show_dir),
				res['name'],
				res['overview'],
				res['thumbnail'],
				thumbnail_width,
				thumbnail_height,
				0,
				datetime.now()
			))
			self._db.commit()

			_print(f"Added {show_name} to DB", LOG_LEVEL_INFO)
		
		cur.close()

	def update_shows(self, show_id=None):
		cur = self._db.cursor(dictionary=True)
		if show_id is not None:
			q = "SELECT id, tvdb_id FROM tv_shows WHERE id = %s"
			cur.execute(q, (show_id, ))
			to_update = cur.fetchall()
		else:
			q = (
				"SELECT id, tvdb_id "
				"FROM tv_shows "
				"WHERE needs_update = 1 "
				"OR last_updated < %s"
			)
			cur.execute(q, (datetime.now() - timedelta(days=7), ))
			to_update = cur.fetchall()
		
		if not to_update:
			_print("Nothing to update", LOG_LEVEL_INFO)
			cur.close()
			return
		
		tvdb = tvdb_v4_official.TVDB(config.TVDB_API_KEY)
		for show in to_update:
			series = tvdb.get_series(show['tvdb_id'])
			thumbnail_width = 0
			thumbnail_height = 0
			if series.get('image', None):
				thumbnail_width, thumbnail_height = get_image_dimensions(series['image'])

			q = (
				"UPDATE tv_shows "
				"SET title = %s, "
				"description = %s, "
				"thumbnail = %s, "
				"thumbnail_width = %s, "
				"thumbnail_height = %s, "
				"last_updated = %s, "
				"needs_update = 0 "
				"WHERE id = %s"
			)
			cur.execute(q, (
				series['name'],
				series['overview'],
				series['image'],
				thumbnail_width,
				thumbnail_height,
				datetime.now(),
				show['id']
			))
			self._db.commit()

	def add_new_episodes(self):
		cur = self._db.cursor(dictionary=True)

		q = "SELECT id, tvdb_id, path FROM tv_shows WHERE verified = 1"
		cur.execute(q)
		res = cur.fetchall()

		tvdb = tvdb_v4_official.TVDB(config.TVDB_API_KEY)

		for r in res:
			show_root = Path(r['path'])
			tvdb_id = r['tvdb_id']
			series = tvdb.get_series(tvdb_id)
			
			episodes = []
			page = 0
			this_page = tvdb.get_series_episodes(tvdb_id, page=page)
			while this_page.get('episodes'):
				episodes += this_page['episodes']
				page +=1
				this_page = tvdb.get_series_episodes(tvdb_id, page=page)

			for current_folder, subfolders, files in os.walk(show_root):
				for file in files:
					full_path = os.path.join(current_folder, file)

					try:
						sp = subprocess.run([
							config.FFPROBE_PATH, "-v", "error", 
							"-show_entries", "format=duration",
							"-of", "default=noprint_wrappers=1:nokey=1", 
							full_path
						], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
						duration = int(float(sp.stdout))
					except:
						_print(f"Error getting duration for folowing file:", LOG_LEVEL_ERROR)
						_print(full_path, LOG_LEVEL_ERROR)
						continue

					q = "SELECT id FROM tv_episodes WHERE path = %s"
					cur.execute(q, (full_path, ))
					res = cur.fetchone()
					if res:
						continue

					file_size = os.path.getsize(full_path) / (1024*1024)
					if file_size < 100:
						_print(f"{full_path} is <100MB, skipping", LOG_LEVEL_INFO)
						continue
					match = re.search(r'S(\d\d)E(\d\d)', file)
					if not match:
						_print(f"Regex didn't match {file}, skipping", LOG_LEVEL_INFO)
						continue

					season_number = int(match.group(1))
					episode_number = int(match.group(2))

					this_episode = None
					for episode in episodes:
						if episode['seasonNumber'] == season_number and episode['number'] == episode_number:
							this_episode = episode
							break
					
					if not this_episode:
						_print(f"Could not match {file} to any episode. Skippping...", LOG_LEVEL_INFO)
						continue

					q = (
						"INSERT INTO tv_episodes "
						"(tv_show_id, "
						"tvdb_id, "
						"path, "
						"duration, "
						"season_number, "
						"episode_number, "
						"description, "
						"last_updated) "
						"values (%s, %s, %s, %s, %s, %s, %s, %s)"
					)
					cur.execute(q, (
						r['id'],
						this_episode['id'],
						full_path,
						duration,
						season_number,
						episode_number,
						this_episode['overview'],
						datetime.now()
					))
					self._db.commit()

		cur.close()

	def remove_missing_episodes(self):
		cur = self._db.cursor(dictionary=True)

		q = (
			"SELECT tv_episodes.*, tv_shows.title "
			"FROM tv_episodes "
			"LEFT JOIN tv_shows "
			"ON tv_episodes.tv_show_id = tv_shows.id"
		)
		cur.execute(q)
		episodes = cur.fetchall()

		for episode in episodes:
			if os.path.exists(episode['path']):
				continue

			_print("Missing Episode:", LOG_LEVEL_INFO)
			_print(f"  {episode['title']} S{episode['season_number']}E{episode['episode_number']}", LOG_LEVEL_INFO)
			_print(f"  {episode['path']}", LOG_LEVEL_INFO)
			q = (
				"SELECT * "
				"FROM tv_episodes "
				"WHERE id != %s "
				"AND tv_show_id = %s "
				"AND season_number = %s "
				"AND episode_number = %s"
			)
			cur.execute(q, (
				episode['id'],
				episode['tv_show_id'],
				episode['season_number'],
				episode['episode_number']
			))
			replacements = cur.fetchall()

			if not replacements:
				_print("No replacement found!", LOG_LEVEL_INFO)
			else:
				_print("Replacement(s):", LOG_LEVEL_INFO)
				for r in replacements:
					_print(f"  {r['path']}", LOG_LEVEL_INFO)
			
			q = "SELECT * FROM schedule WHERE path = %s AND end_time >= NOW()"
			cur.execute(q, (episode['path'], ))
			scheduled = cur.fetchall()

			if scheduled:
				_print("Schedule exists:", LOG_LEVEL_INFO)
				for s in scheduled:
					_print(s, LOG_LEVEL_INFO)
					q = "DELETE FROM schedule WHERE id = %s"
					cur.execute(q, (s['id'], ))
					_print("Removed", LOG_LEVEL_INFO)

			q = "DELETE FROM tv_episodes WHERE id = %s"
			cur.execute(q, (episode['id'], ))
			self._db.commit()

		cur.close()

	def cleanup_last_played_episodes(self):
		cur = self._db.cursor(dictionary=True)

		q = (
			"SELECT id, last_played_episode, title "
			"FROM tv_shows "
			"WHERE verified = 1 "
			"AND enabled = 1"
		)
		cur.execute(q)
		shows = cur.fetchall()

		for show in shows:
			## This is a shitty thing to do with this path join
			## File paths should realistically be stored in their own table
			q = (
				"SELECT tv_episodes.id AS episode_id "
				"FROM schedule "
				"LEFT JOIN tv_episodes "
				"ON schedule.path = tv_episodes.path "
				"LEFT JOIN tv_shows "
				"ON tv_episodes.tv_show_id = tv_shows.id "
				"WHERE tv_shows.id = %s "
				"ORDER BY schedule.start_time DESC "
				"LIMIT 1"
			)
			cur.execute(q, (show['id'], ))
			last_played = cur.fetchone()
			last_played_episode_id = None
			if last_played:
				last_played_episode_id = last_played['episode_id']

			if last_played_episode_id == show['last_played_episode']:
				_print(f"Last played matches for {show['title']}", LOG_LEVEL_INFO)
			else:
				_print(f"Mismatch for {show['title']}! Fixing...", LOG_LEVEL_INFO)
				q = "UPDATE tv_shows SET last_played_episode = %s WHERE id = %s"
				cur.execute(q, (last_played_episode_id, show['id']))
		self._db.commit()	

		cur.close()