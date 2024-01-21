import random
from datetime import datetime, timedelta, time as dttime
import pytz
from tzlocal import get_localzone
from xml.dom import minidom

from lib.common import get_mysql_connection
import config

class Scheduler:
	def __init__(self):
		self._db = get_mysql_connection()

	def close(self):
		self._db.close()

	def get_next_episode(self, tv_show_id):
		cur = self._db.cursor(dictionary=True)
		q = "SELECT last_played_episode FROM tv_shows WHERE id = %s"
		cur.execute(q, (tv_show_id, ))
		res = cur.fetchone()

		last_played = res['last_played_episode']
		if last_played is None:
			#q = "SELECT * FROM tv_episodes WHERE tv_show_id = %s AND transcoded = 1 ORDER BY season_number, episode_number LIMIT 1"
			q = "SELECT * FROM tv_episodes WHERE tv_show_id = %s ORDER BY season_number, episode_number LIMIT 1"
			cur.execute(q, (tv_show_id, ))
			next_episode = cur.fetchone()
			cur.close()
			return next_episode
		
		q = "SELECT season_number, episode_number FROM tv_episodes WHERE id = %s"
		cur.execute(q, (last_played, ))
		res = cur.fetchone()
		last_season = res['season_number']
		last_episode = res['episode_number']

		#q = "SELECT * FROM tv_episodes WHERE tv_show_id = %s AND season_number = %s AND episode_number > %s AND transcoded = 1 ORDER BY episode_number LIMIT 1"
		q = "SELECT * FROM tv_episodes WHERE tv_show_id = %s AND season_number = %s AND episode_number > %s ORDER BY episode_number LIMIT 1"
		cur.execute(q, (tv_show_id, last_season, last_episode))
		res = cur.fetchone()
		if res:
			cur.close()
			return res
		
		#q = "SELECT * FROM tv_episodes WHERE tv_show_id = %s AND season_number > %s AND transcoded = 1 ORDER BY season_number, episode_number LIMIT 1"
		q = "SELECT * FROM tv_episodes WHERE tv_show_id = %s AND season_number > %s ORDER BY season_number, episode_number LIMIT 1"
		cur.execute(q, (tv_show_id, last_season))
		res = cur.fetchone()
		if res:
			cur.close()
			return res
		
		#q = "SELECT * FROM tv_episodes WHERE tv_show_id = %s AND transcoded = 1 ORDER BY season_number, episode_number LIMIT 1"
		q = "SELECT * FROM tv_episodes WHERE tv_show_id = %s ORDER BY season_number, episode_number LIMIT 1"
		cur.execute(q, (tv_show_id, ))
		res = cur.fetchone()
		cur.close()
		return res

	def build_schedule(self, date=datetime.now().date()+timedelta(days=1)):
		start_time = datetime.combine(date, dttime(4))
		cur = self._db.cursor(dictionary=True)

		q = "SELECT * FROM schedule WHERE start_time >= %s AND start_time < %s LIMIT 1"
		cur.execute(q, (start_time, start_time + timedelta(hours=24)))
		res = cur.fetchone()
		if res:
			print(f"Scheduled items already exist for {date}")
			cur.close()
			return

		marathon_show = None
		marathon_data = {}
		if random.random() <= config.MARATHON_CHANCE:
			## All shows that have >= 20h of content
			#s = "SELECT SUM(duration) AS total_duration, tv_show_id FROM tv_episodes LEFT JOIN tv_shows ON tv_episodes.tv_show_id = tv_shows.id WHERE tv_shows.enabled = 1 AND tv_episodes.transcoded = 1 GROUP BY tv_show_id HAVING total_duration >= 72000 ORDER BY RAND() LIMIT 1"
			s = "SELECT SUM(duration) AS total_duration, tv_show_id FROM tv_episodes LEFT JOIN tv_shows ON tv_episodes.tv_show_id = tv_shows.id WHERE tv_shows.enabled = 1 GROUP BY tv_show_id HAVING total_duration >= 72000 ORDER BY RAND() LIMIT 1"
			cur.execute(s)
			res = cur.fetchone()
			if res:
				s = "SELECT * FROM tv_shows WHERE id = %s"
				cur.execute(s, (res['tv_show_id'], ))
				marathon_show = cur.fetchone()

		if marathon_show is not None:
			## Start the marathon with >= 12h left in the day
			marathon_start = random.randint(0, 43200)
			## Duration is 8-12 hours
			marathon_duration = random.randint(28800, 43200)
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
				if previous_show is None:
					if marathon_show is None:
						q = "SELECT id FROM tv_shows WHERE enabled = 1 ORDER BY RAND() LIMIT 1"
						cur.execute(q)
					else:
						q = "SELECT id FROM tv_shows WHERE enabled = 1 AND id != %s ORDER BY RAND() LIMIT 1"
						cur.execute(q, (marathon_show['id'], ))
				else:
					if marathon_show is None:
						q = "SELECT id FROM tv_shows WHERE enabled = 1 AND id != %s ORDER BY RAND() LIMIT 1"
						cur.execute(q, (previous_show, ))
					else:
						q = "SELECT id FROM tv_shows WHERE enabled = 1 AND id != %s AND id != %s ORDER BY RAND() LIMIT 1"
						cur.execute(q, (previous_show, marathon_show['id']))
				res = cur.fetchone()
				current_show_id = res['id']
			elif in_marathon:
				current_show_id = marathon_show['id']

			next_episode = self.get_next_episode(current_show_id)
			if not next_episode:
				current_show_counter = 0
				current_show_repeats = 0
				continue

			## See if we entered MARATHON TIME
			if marathon_show is not None and total_duration + next_episode['duration'] > marathon_data['start'] and not in_marathon and marathon_timer == 0:
				in_marathon = True
				current_show_counter = 0
				current_show_repeats = 0
				continue

			## If we are in a marathon and this one exceeds the timer, go back to normal
			if in_marathon and marathon_timer + next_episode['duration'] > marathon_data['duration']:
				in_marathon = False
				continue

			## break if we are going to exceed 24h
			if total_duration + next_episode['duration'] > 86400:
				break
			
			if current_show_counter >= current_show_repeats:
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

			q = "INSERT INTO schedule (tv_episode_id, start_time, end_time) VALUES (%s, %s, %s)"
			cur.execute(q, (next_episode['id'], episode_start_time, episode_end_time))

			q = "UPDATE tv_shows SET last_played_episode = %s WHERE id = %s"
			cur.execute(q, (next_episode['id'], next_episode['tv_show_id']))
			self._db.commit()

		cur.close()

	def generate_xmltv(self, output_file):
		cur = self._db.cursor(dictionary=True)
		schedule_start = datetime.combine((datetime.now() - timedelta(days=1)).date(), dttime(0))
		
		q = "SELECT schedule.*, tv_episodes.season_number, tv_episodes.episode_number, tv_episodes.description AS episode_description, tv_shows.title AS show_title, tv_shows.description AS show_description, tv_shows.thumbnail, tv_shows.thumbnail_height, tv_shows.thumbnail_width FROM schedule LEFT JOIN tv_episodes ON schedule.tv_episode_id = tv_episodes.id LEFT JOIN tv_shows ON tv_episodes.tv_show_id = tv_shows.id WHERE schedule.start_time >= %s"
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
			title = f"{s['show_title']} S{s['season_number']} E{s['episode_number']}"
			title_element.appendChild(root.createTextNode(title))

			description = s['episode_description']
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