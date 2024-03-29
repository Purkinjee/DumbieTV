import random
from datetime import datetime, timedelta, time as dttime
import pytz
from tzlocal import get_localzone
from xml.dom import minidom

from lib.common import get_mysql_connection, Logger
import config
from lib.vars import *

_print = Logger()._print

class Scheduler:
	def __init__(self, logger=None):
		self._db = get_mysql_connection()

		if logger is not None:
			global _print
			_print = logger._print

	def close(self):
		self._db.close()

	def get_next_episode(self, tv_show_id):
		cur = self._db.cursor(dictionary=True)
		q = "SELECT last_played_episode FROM tv_shows WHERE id = %s"
		cur.execute(q, (tv_show_id, ))
		res = cur.fetchone()

		last_played = res['last_played_episode']
		if last_played is None:
			q = (
				"SELECT * FROM tv_episodes "
				"WHERE tv_show_id = %s "
				"ORDER BY season_number, episode_number "
				"LIMIT 1"
			)
			cur.execute(q, (tv_show_id, ))
			next_episode = cur.fetchone()
			cur.close()
			return next_episode
		
		q = (
			"SELECT season_number, episode_number "
			"FROM tv_episodes "
			"WHERE id = %s"
		)
		cur.execute(q, (last_played, ))
		res = cur.fetchone()
		last_season = res['season_number']
		last_episode = res['episode_number']

		q = (
			"SELECT * FROM tv_episodes "
			"WHERE tv_show_id = %s "
			"AND season_number = %s "
			"AND episode_number > %s "
			"ORDER BY episode_number "
			"LIMIT 1"
		)
		cur.execute(q, (tv_show_id, last_season, last_episode))
		res = cur.fetchone()
		if res:
			cur.close()
			return res
		
		q = (
			"SELECT * FROM tv_episodes "
			"WHERE tv_show_id = %s "
			"AND season_number > %s "
			"ORDER BY season_number, episode_number "
			"LIMIT 1"
		)
		cur.execute(q, (tv_show_id, last_season))
		res = cur.fetchone()
		if res:
			cur.close()
			return res
		
		q = (
			"SELECT * FROM tv_episodes "
			"WHERE tv_show_id = %s "
			"ORDER BY season_number, episode_number "
			"LIMIT 1"
		)
		cur.execute(q, (tv_show_id, ))
		res = cur.fetchone()
		cur.close()
		return res

	def build_schedule(self, date=datetime.now().date()+timedelta(days=1)):
		start_time = datetime.combine(date, dttime(0))
		cur = self._db.cursor(dictionary=True)

		q = (
			"SELECT * FROM schedule "
			"WHERE end_time >= %s "
			"ORDER BY end_time DESC "
			"LIMIT 1"
		)
		cur.execute(q, (start_time, ))
		schedule_end = cur.fetchone()

		if schedule_end:
			if schedule_end['end_time'].date() > date:
				_print(f"Scheduled items already exist for {date}", LOG_LEVEL_ERROR)
				cur.close()
				return
			else:
				start_time = schedule_end['end_time']

		marathon_show = None
		marathon_data = {}
		if random.random() <= config.MARATHON_CHANCE:
			## All shows that have >= 20h of content
			s = (
				"SELECT SUM(duration) AS total_duration, tv_show_id "
				"FROM tv_episodes "
				"LEFT JOIN tv_shows "
				"ON tv_episodes.tv_show_id = tv_shows.id "
				"WHERE tv_shows.enabled = 1 "
				"GROUP BY tv_show_id "
				"HAVING total_duration >= 72000 "
				"ORDER BY RAND() "
				"LIMIT 1"
			)
			cur.execute(s)
			res = cur.fetchone()
			if res:
				s = "SELECT * FROM tv_shows WHERE id = %s"
				cur.execute(s, (res['tv_show_id'], ))
				marathon_show = cur.fetchone()

		if marathon_show is not None:
			## Make sure we have at least 8 hours of open schedule this day
			schedule_end_time = datetime.combine(date + timedelta(days=1), dttime(0))
			time_left_in_day = (schedule_end_time - start_time).total_seconds()
			## Not enough time for a marathon
			if time_left_in_day < 28800:
				marathon_show = None
			else:
				## Max duration is 12 hours unless that much time isn't left in the day
				max_duration = min(time_left_in_day, 43200)
				## Duration is 8-12 hours
				marathon_duration = random.randint(28800, max_duration)
				## Start the marathon with enough time left in the day
				marathon_start = random.randint(0, time_left_in_day - marathon_duration)
				
				marathon_data = {
					'start': marathon_start,
					'duration': marathon_duration
				}

		total_duration = 0
		previous_show = None
		current_show_counter = 0
		current_show_repeats = 0
		current_show_id = None
		in_marathon = False
		marathon_timer = 0
		while True:
			if current_show_id is None or current_show_counter >= current_show_repeats and not in_marathon:
				current_show_counter = 0
				current_show_repeats = 0
				meta = {
					'show_name': None,
					'title': 'Unknown',
					'description': 'No Description',
					'thumbnail': None,
					'thumbnail_height': 0,
					'thumbnail_width': 0
				}
				if previous_show is None:
					if marathon_show is None:
						q = (
							"SELECT id, title, thumbnail, thumbnail_width, thumbnail_height "
							"FROM tv_shows "
							"WHERE enabled = 1 "
							"ORDER BY RAND() "
							"LIMIT 1"
						)
						cur.execute(q)
					else:
						q = (
							"SELECT id, title, thumbnail, thumbnail_width, thumbnail_height "
							"FROM tv_shows "
							"WHERE enabled = 1 "
							"AND id != %s "
							"ORDER BY RAND() "
							"LIMIT 1"
						)
						cur.execute(q, (marathon_show['id'], ))
				else:
					if marathon_show is None:
						q = (
							"SELECT id, title, thumbnail, thumbnail_width, thumbnail_height "
							"FROM tv_shows "
							"WHERE enabled = 1 "
							"AND id != %s "
							"ORDER BY RAND() "
							"LIMIT 1"
						)
						cur.execute(q, (previous_show, ))
					else:
						q = (
							"SELECT id, title, thumbnail, thumbnail_width, thumbnail_height "
							"FROM tv_shows "
							"WHERE enabled = 1 "
							"AND id != %s "
							"AND id != %s "
							"ORDER BY RAND() "
							"LIMIT 1"
						)
						cur.execute(q, (previous_show, marathon_show['id']))
				res = cur.fetchone()
				current_show_id = res['id']
				meta['show_name'] = res['title']
				meta['thumbnail'] = res['thumbnail']
				meta['thumbnail_height'] = res['thumbnail_height']
				meta['thumbnail_width'] = res['thumbnail_width']
			elif in_marathon:
				current_show_id = marathon_show['id']
				meta['show_name'] = marathon_show['title']
				meta['thumbnail'] = marathon_show['thumbnail']
				meta['thumbnail_height'] = marathon_show['thumbnail_height']
				meta['thumbnail_width'] = marathon_show['thumbnail_width']

			next_episode = self.get_next_episode(current_show_id)
			if not next_episode:
				current_show_counter = 0
				current_show_repeats = 0
				continue

			## See if we entered MARATHON TIME
			if (marathon_show is not None 
				and total_duration + next_episode['duration'] > marathon_data['start'] 
				and not in_marathon 
				and marathon_timer == 0):

				in_marathon = True
				current_show_counter = 0
				current_show_repeats = 0
				continue

			## If we are in a marathon and this one exceeds the timer, go back to normal
			if in_marathon and marathon_timer + next_episode['duration'] > marathon_data['duration']:
				in_marathon = False
				continue

			meta['description'] = next_episode['description']
			
			if current_show_counter >= current_show_repeats:
				current_show_counter = 0
				## Allow more repeats of shorter shows
				if next_episode['duration'] > 1800:
					possible_repeats = [2]
				else:
					possible_repeats = [2,4]
				
				if random.random() < 0.4:
					current_show_repeats = possible_repeats[random.randint(0, len(possible_repeats)-1)]
				else:
					current_show_repeats = 0
			
			episode_start_time = start_time + timedelta(seconds=total_duration)
			episode_end_time = start_time + timedelta(seconds=(total_duration + next_episode['duration']))

			previous_show = next_episode['tv_show_id']
			current_show_counter += 1
			total_duration += next_episode['duration']

			if in_marathon:
				marathon_timer += next_episode['duration']
				meta['title'] = f"{meta['show_name']} Marathon! S{next_episode['season_number']} E{next_episode['episode_number']}"
			else:
				meta['title'] = f"{meta['show_name']} S{next_episode['season_number']} E{next_episode['episode_number']}"

			q = (
				"INSERT INTO schedule "
				"(tv_episode_id, start_time, end_time, is_marathon, "
				"title, description, path, thumbnail, "
				"thumbnail_height, thumbnail_width) "
				"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
			)
			cur.execute(q, (
				next_episode['id'], 
				episode_start_time, 
				episode_end_time, 
				in_marathon*1,
				meta['title'],
				meta['description'],
				next_episode['path'],
				meta['thumbnail'],
				meta['thumbnail_height'],
				meta['thumbnail_width']
			))

			q = "UPDATE tv_shows SET last_played_episode = %s WHERE id = %s"
			cur.execute(q, (next_episode['id'], next_episode['tv_show_id']))
			self._db.commit()

			if episode_end_time.date() > date:
				break

		cur.close()

	def generate_xmltv(self, output_file):
		cur = self._db.cursor(dictionary=True)
		schedule_start = datetime.combine((datetime.now() - timedelta(days=1)).date(), dttime(0))
		
		q = "SELECT * FROM schedule WHERE start_time >= %s"
		cur.execute(q, (schedule_start, ))
		schedule = cur.fetchall()

		root = minidom.Document()
		tv_element = root.createElement('tv')
		root.appendChild(tv_element)

		channel_element = root.createElement('channel')
		channel_element.setAttribute("id", config.CHANNEL_NUMBER)
		display_name_element = root.createElement('display-name')
		display_name_element.appendChild(root.createTextNode(config.CHANNEL_NAME))
		channel_element.appendChild(display_name_element)
		if config.CHANNEL_ICON:
			icon_element = root.createElement('icon')
			icon_element.setAttribute("src", config.CHANNEL_ICON)
			icon_element.setAttribute("width", "100")
			icon_element.setAttribute("height", "100")
			channel_element.appendChild(icon_element)

		tv_element.appendChild(channel_element)

		for s in schedule:
			tz = pytz.timezone(config.TIMEZONE)
			start_aware = tz.localize(s['start_time'])
			end_aware = tz.localize(s['end_time'])

			program_element = root.createElement('programme')
			program_element.setAttribute("start", start_aware.strftime("%Y%m%d%H%M%S %z"))
			program_element.setAttribute("stop", end_aware.strftime("%Y%m%d%H%M%S %z"))
			program_element.setAttribute("channel", config.CHANNEL_NUMBER)

			title_element = root.createElement("title")
			title_element.setAttribute("lang", "en")
			title_element.appendChild(root.createTextNode(s['title']))

			description = s['description']
			if description is None:
				description = "No description"
			desc_element = root.createElement("desc")
			desc_element.setAttribute("lang", "en")
			desc_element.appendChild(root.createTextNode(description))
			

			program_element.appendChild(title_element)
			program_element.appendChild(desc_element)

			if s['thumbnail'] and s['thumbnail_width'] and s['thumbnail_height']:
				icon_element = root.createElement('icon')
				icon_element.setAttribute('src', s['thumbnail'])
				icon_element.setAttribute('width', str(s['thumbnail_width']))
				icon_element.setAttribute('height', str(s['thumbnail_height']))
				program_element.appendChild(icon_element)

			tv_element.appendChild(program_element)


		xml_str = root.toprettyxml(indent = "  ")
		with open(output_file, 'w') as f:
			f.write(xml_str)

		cur.close()
	
	def fix(self):
		cur = self._db.cursor(dictionary=True)
		q = "SELECT * FROM schedule"
		cur.execute(q)
		schedule = cur.fetchall()

		for s in schedule:
			q = (
				"SELECT tv_episodes.path, tv_episodes.description AS episode_desc, "
				"tv_shows.thumbnail, tv_shows.thumbnail_height, "
				"tv_shows.thumbnail_width, tv_shows.title, "
				"tv_shows.description AS show_desc, tv_episodes.season_number, "
				"tv_episodes.episode_number "
				"FROM tv_episodes "
				"LEFT JOIN tv_shows "
				"ON tv_episodes.tv_show_id = tv_shows.id where tv_episodes.id = %s "
				"LIMIT 1"
			)
			cur.execute(q, (s['tv_episode_id'],))
			data = cur.fetchone()

			title = f"{data['title']} S{data['season_number']} E{data['episode_number']}"
			
			q = (
				"UPDATE schedule "
				"SET title = %s, "
				"description = %s, "
				"path = %s, "
				"thumbnail = %s, "
				"thumbnail_height = %s, "
				"thumbnail_width = %s "
				"WHERE id = %s"
			)
			cur.execute(q, (
				title, 
				data['episode_desc'], 
				data['path'], 
				data['thumbnail'],
				data['thumbnail_height'],
				data['thumbnail_width'],
				s['id']
			))

		self._db.commit()
		cur.close()

	def adjust_schedule_times(self):
		cur = self._db.cursor(dictionary=True)

		q = (
			"SELECT * "
			"FROM schedule "
			"WHERE completed = 1 "
			"AND actual_end_time IS NOT NULL "
			"ORDER BY start_time DESC "
			"LIMIT 1"
		)
		cur.execute(q)
		recent_finish = cur.fetchone()

		if not recent_finish:
			_print("Nothing has ever played?", LOG_LEVEL_ERROR)
			cur.close()
			return
		
		if recent_finish['end_time'] == recent_finish['actual_end_time']:
			_print("Times already match", LOG_LEVEL_INFO)
			cur.close()
			return
		
		offset = (recent_finish['actual_end_time'] - recent_finish['end_time']).total_seconds()
		_print(f"Offset is {offset}s", LOG_LEVEL_INFO)

		q = (
			"SELECT * "
			"FROM schedule "
			"WHERE start_time > NOW() "
			"AND actual_start_time IS NULL "
			"AND completed = 0 "
			"ORDER BY start_time"
		)
		cur.execute(q)
		future_items = cur.fetchall()

		previous = None
		for schedule in future_items:
			if previous and previous['end_time'] < schedule['start_time']:
				_print(f"Gap in schedule found at {schedule['start_time']}. No further adjustments made", LOG_LEVEL_INFO)
				break
			q = "UPDATE schedule SET start_time = %s, end_time = %s WHERE id = %s"
			cur.execute(q, (
				schedule['start_time'] + timedelta(seconds=offset),
				schedule['end_time'] + timedelta(seconds=offset),
				schedule['id']
			))
			previous = schedule

		self._db.commit()
		cur.close()