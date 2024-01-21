import os
import re
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

from imdb import Cinemagoer
import tvdb_v4_official

from lib.common import get_mysql_connection, get_image_dimensions
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
				print(f"{str(show_dir)} exists in DB, skipping...")
				continue


			show_name = show_dir.parts[-1]
			res = tvdb.search(show_name)
			if not res:
				print(f"Could not retrieve TVDB info for {show_name}")
				continue
			res = res[0]

			thumbnail_width = 0
			thumbnail_height = 0
			if res.get('image', None):
				thumbnail_width, thumbnail_height = get_image_dimensions(series['image'])

			q = "INSERT INTO tv_shows (tvdb_id, path, title, description, thumbnail, thumbnail_width, thumbnail_height, verified, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
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

			print(f"Added {show_name} to DB")
		
		cur.close()

	def update_shows(self, show_id=None):
		cur = self._db.cursor(dictionary=True)
		if show_id is not None:
			q = "SELECT id, tvdb_id FROM tv_shows WHERE id = %s"
			cur.execute(q, (show_id, ))
			to_update = cur.fetchall()
		else:
			q = "SELECT id, tvdb_id FROM tv_shows WHERE needs_update = 1 OR last_updated < %s"
			cur.execute(q, (datetime.now() - timedelta(days=7), ))
			to_update = cur.fetchall()
		
		if not to_update:
			print("Nothing to update")
			cur.close()
			return
		
		tvdb = tvdb_v4_official.TVDB(config.TVDB_API_KEY)
		for show in to_update:
			series = tvdb.get_series(show['tvdb_id'])
			thumbnail_width = 0
			thumbnail_height = 0
			if series.get('image', None):
				thumbnail_width, thumbnail_height = get_image_dimensions(series['image'])

			q = "UPDATE tv_shows SET title = %s, description = %s, thumbnail = %s, thumbnail_width = %s, thumbnail_height = %s, last_updated = %s, needs_update = 0 WHERE id = %s"
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
			episodes = tvdb.get_series_episodes(tvdb_id)['episodes']
			for current_folder, subfolders, files in os.walk(show_root):
				for file in files:
					full_path = os.path.join(current_folder, file)

					sp = subprocess.run([
						"ffprobe", "-v", "error", "-show_entries", "format=duration",
						"-of", "default=noprint_wrappers=1:nokey=1", full_path
					], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
					duration = int(float(sp.stdout))

					q = "SELECT id FROM tv_episodes WHERE path = %s"
					cur.execute(q, (full_path, ))
					res = cur.fetchone()
					if res:
						#print(f"{full_path} exists in DB, skipping...")
						continue

					file_size = os.path.getsize(full_path) / (1024*1024)
					if file_size < 100:
						print(f"{full_path} is <100MB, skipping")
						continue
					match = re.search(r'S(\d\d)E(\d\d)', file)
					if not match:
						print(f"Regex didn't match {file}, skipping")
						continue

					season_number = int(match.group(1))
					episode_number = int(match.group(2))

					this_episode = None
					for episode in episodes:
						if episode['seasonNumber'] == season_number and episode['number'] == episode_number:
							this_episode = episode
							break
					
					if not this_episode:
						print(f"Could not match {file} to any episode. Skippping...")
						continue

					q = "INSERT INTO tv_episodes (tv_show_id, tvdb_id, path, duration, season_number, episode_number, description, last_updated) values (%s, %s, %s, %s, %s, %s, %s, %s)"
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

		q = "SELECT tv_episodes.*, tv_shows.title FROM tv_episodes LEFT JOIN tv_shows ON tv_episodes.tv_show_id = tv_shows.id"
		cur.execute(q)
		episodes = cur.fetchall()

		for episode in episodes:
			if os.path.exists(episode['path']):
				continue

			print("Missing Episode:")
			print(f"  {episode['title']} S{episode['season_number']}E{episode['episode_number']}")
			print(f"  {episode['path']}")
			q = "SELECT * FROM tv_episodes WHERE id != %s AND tv_show_id = %s AND season_number = %s AND episode_number = %s"
			cur.execute(q, (
				episode['id'],
				episode['tv_show_id'],
				episode['season_number'],
				episode['episode_number']
			))
			replacements = cur.fetchall()

			if not replacements:
				print("No replacement found!")
			else:
				print("Replacement(s):")
				for r in replacements:
					print(f"  {r['path']}")
			
			q = "SELECT * FROM schedule WHERE tv_episode_id = %s AND start_time >= NOW()"
			cur.execute(q, (episode['id'], ))
			scheduled = cur.fetchall()

			if scheduled:
				print("Schedule exists:")
				for s in scheduled:
					print(s)
					q = "DELETE FROM schedule WHERE id = %s"
					cur.execute(q, (s['id'], ))
					print("Removed")

			q = "DELETE FROM tv_episodes WHERE id = %s"
			cur.execute(q, (episode['id'], ))
			self._db.commit()
			
			print(' ')

		cur.close()