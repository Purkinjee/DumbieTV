from datetime import datetime, timedelta, time as dttime
import time
import os, sys, signal, random
import subprocess
import re

import pyht
from gtts import gTTS
from pyt2s.services import ibm_watson
from pydub import AudioSegment

from lib.common import get_mysql_connection, Logger
from lib.vars import *
import config

_print = Logger()._print

def normalize_text(t):
	return t\
	.replace("\\", "\\\\\\\\")\
	.replace('"', '\\\\"')\
	.replace("'", "\\\\\\'")\
	.replace("%", "\\\\\\\\%")\
	.replace(":", "\\\\:")\
	.replace(",", "\\\\\\,")

class Intermission:
	def __init__(self, logger=None):
		self._setup_output_dirs()
		if logger is not None:
			global _print
			_print = logger._print
		
	
	def close(self):
		pass

	def generate_all_future_intermissions(self):
		db = get_mysql_connection()
		cur = db.cursor(dictionary=True)

		q = (
			"SELECT * FROM schedule "
			"WHERE start_time > %s "
			"AND tag = 'INTERMISSION' "
			"AND path IS NULL"
		)
		cur.execute(q, (datetime.now(), ))
		intermissions = cur.fetchall()
		_print(f"Need to generate {len(intermissions)} intermission(s)", LOG_LEVEL_INFO)

		for i in intermissions:
			q = (
				"SELECT id FROM schedule "
				"WHERE start_time >= %s "
				"AND tag != 'INTERMISSION' "
				"LIMIT 4"
			)
			cur.execute(q, (i['end_time'],))
			res = cur.fetchall()
			if len(res) < 4:
				_print(f'Not enough future scheduled items for intermission {i["id"]}', LOG_LEVEL_DEBUG)
				continue

			intermission_file = self.generate_intermission_video(i['id'])
			_print(f"Generated intermission {intermission_file}", LOG_LEVEL_INFO)
			q = (
				"UPDATE schedule "
				"SET path = %s "
				"WHERE id = %s"
			)
			cur.execute(q, (intermission_file, i['id']))
			db.commit()

		cur.close()
		db.close()

	def delete_old_intermissions(self):
		db = get_mysql_connection()
		cur = db.cursor(dictionary=True)

		q = (
			"SELECT * FROM schedule "
			"WHERE end_time < %s "
			"AND tag = 'INTERMISSION' "
			"AND path IS NOT NULL"
		)
		cur.execute(q, (datetime.now(), ))
		old = cur.fetchall()

		_print(f"{len(old)} intermission videos need to be removed", LOG_LEVEL_INFO)

		for s in old:
			os.remove(s['path'])
			q = (
				"UPDATE schedule "
				"SET path = NULL "
				"WHERE id = %s"
			)
			cur.execute(q, (s['id'], ))
			db.commit()

		cur.close()
		db.close()

	def generate_intermission_video(self, schedule_id):
		voiceover_file = self._generate_speech_pyt2s(schedule_id)
		if not voiceover_file:
			_print("Error generating speech. Aborting video creation", LOG_LEVEL_ERROR)
			return
		audio_track = self._generate_audio_track(schedule_id, voiceover_file)
		if not audio_track:
			os.remove(voiceover_file)
			_print("Error generating audio track. Aborting video creation", LOG_LEVEL_ERROR)
			return
		db = get_mysql_connection()
		cur = db.cursor(dictionary=True)

		q = (
			"SELECT * FROM schedule "
			"WHERE id = %s"
		)
		cur.execute(q, (schedule_id,))
		intermission_schedule = cur.fetchone()

		if not intermission_schedule:
			_print(f'Cannot find scheduled item: {schedule_id}', LOG_LEVEL_ERROR)
			cur.close()
			db.close()
			return False
		
		q = (
			"SELECT * FROM schedule "
			"WHERE start_time >= %s "
			"AND tag != 'INTERMISSION' "
			"LIMIT 4"
		)
		cur.execute(q, (intermission_schedule['end_time'],))
		future_schedule = cur.fetchall()

		filters = ""
		overlay_font = os.path.join(config.INTERMISSION_RESOURCE_PATH, 'fonts/SairaCondensed-Regular.ttf')
		overlay_font_bold = os.path.join(config.INTERMISSION_RESOURCE_PATH, 'fonts/SairaCondensed-SemiBold.ttf')
		y_pos = 150
		x_pos = 600
		for schedule in future_schedule:
			start_time = schedule['start_time'].strftime("%-I\\\\\:%M")

			line_1 = schedule['title']
			line_2 = ""

			match = re.search(r'^(.+)S(\d+)\s*E(\d+)\s*$', schedule['title'])
			if match:
				title = match.group(1).strip()
				season = match.group(2)
				episode = match.group(3)
				line_1 = title
				line_2 = f"Season {season} Episode {episode}"

			line_1 = normalize_text(line_1)
			line_2 = normalize_text(line_2)

			if filters:
				filters += ","

			filters += (
				f"drawtext=fontfile={overlay_font_bold}:"
				f"text={start_time}:"
				"fontsize=72:"
				"fontcolor=white:"
				f"x={x_pos}:y={y_pos},"
			)

			y_pos += 63
			filters += (
				f"drawtext=fontfile={overlay_font}:"
				f"text={line_1}:"
				"fontsize=56:"
				"fontcolor=white:"
				f"x={x_pos}:y={y_pos}"
			)

			if line_2:
				y_pos += 58
				filters += (
					f",drawtext=fontfile={overlay_font}:"
					f"text={line_2}:"
					"fontsize=38:"
					f"x={x_pos}:y={y_pos}:"
					"fontcolor=white"
				)
			
			y_pos += 100

		intermission_file = os.path.join(config.INTERMISSION_OUTPUT_PATH, 'complete/', f"{intermission_schedule['id']}.mp4")
		ffmpeg_params = [
			config.FFMPEG_PATH,
			'-hwaccel_output_format', 'cuda',
			'-i', os.path.join(config.INTERMISSION_RESOURCE_PATH, 'background.mp4'),
			'-i', audio_track,
			'-vf', filters,
			'-c:v', 'h264_nvenc',
			'-pix_fmt', 'yuv420p',
			'-r', '30000/1001',
			'-c:a', 'aac',
			'-ar', '44100',
			'-b:a', "256k",
			'-ac', "1",
			'-y',
			intermission_file
		]

		#ffmpeg_process = subprocess.Popen(ffmpeg_params)
		ffmpeg_process = subprocess.Popen(
			ffmpeg_params, 
			stdout=subprocess.DEVNULL, 
			stderr=subprocess.STDOUT
		)
		while ffmpeg_process.poll() is None:
			time.sleep(1)

		os.remove(voiceover_file)
		os.remove(audio_track)
		cur.close()
		db.close()

		return intermission_file
	
	def generate_voiceover_text(self, schedule_id, future_items=4):
		db = get_mysql_connection()
		cur = db.cursor(dictionary=True)
		voiceover_str = ""

		q = (
			"SELECT * FROM schedule "
			"WHERE id = %s"
		)
		cur.execute(q, (schedule_id, ))
		intermission_schedule = cur.fetchone()

		if not intermission_schedule:
			_print(f'Cannot find scheduled item: {schedule_id}', LOG_LEVEL_ERROR)
			cur.close()
			db.close()
			return ""

		q = (
			"SELECT * FROM schedule "
			"WHERE end_time <= %s "
			"AND tag != 'INTERMISSION' "
			"ORDER BY end_time DESC "
			"LIMIT 1"
		)
		cur.execute(q, (intermission_schedule['start_time'], ))
		previous_schedule = cur.fetchone()

		q = (
			"SELECT * FROM schedule "
			"WHERE start_time >= %s "
			"AND tag != 'INTERMISSION' "
			"LIMIT %s"
		)
		cur.execute(q, (intermission_schedule['end_time'], future_items))
		future_schedule = cur.fetchall()

		if not future_schedule:
			_print("No future schedule items found", LOG_LEVEL_ERROR)
			cur.close()
			db.close()
			return ""

		repeats = 0
		if previous_schedule and previous_schedule['tag'] != 'MOVIE' and not previous_schedule['is_marathon']:
			previous_show_name = self._get_show_name(previous_schedule['title'], tts_clean=True)

			for f in future_schedule:
				next_show_name = self._get_show_name(f['title'], tts_clean=True)
				if previous_show_name != next_show_name:
					break
				repeats += 1
		
		if repeats == 1 and previous_show_name:
			voiceover_str += f"Up next is another episode of {previous_show_name}"
		elif repeats > 1 and previous_show_name:
			voiceover_str += f"Coming up is {repeats} more episodes of {previous_show_name}"
		
		remaining = future_schedule[repeats:]
		if len(remaining) == 0:
			voiceover_str = voiceover_str.strip() + "."
		elif len(remaining) == 1:
			last = remaining[0]
			if last['tag'] == 'MOVIE':
				if voiceover_str:
					voiceover_str += f", and then {last['title']}"
				else:
					voiceover_str += f"Up next is {last['title']}"
			if last['is_marathon']:
				title = self._get_show_name(last['title'], tts_clean=True)
				if voiceover_str:
					voiceover_str += f", then a {title} marathon"
				else:
					voiceover_str += f"Coming up is a {title} marathon"
			else:
				title = self._get_show_name(last['title'], tts_clean=True)
				if voiceover_str:
					voiceover_str += f", then an episode of {title}"
				else:
					voiceover_str += f"Coming up is an episode of {title}"
		else:
			i = 0
			while len(remaining) > i:
				if remaining[i]['tag'] != 'MOVIE':
					this_title = self._get_show_name(remaining[i]['title'], tts_clean=True)
					this_record = remaining[i]
					repeats = 1
					x = i+1
					while len(remaining) > x:
						next_title = self._get_show_name(remaining[x]['title'], tts_clean=True)
						if next_title == this_title:
							repeats += 1
							i += 1
							x += 1
						else:
							break
					if repeats > 1:
						## Last Record
						if len(remaining) <= i+1:
							if this_record['is_marathon']:
								if voiceover_str:
									voiceover_str += f", followed by a {this_title} marathon"
								elif previous_schedule['is_marathon']:
									voiceover_str += f"Coming up we will continue our marathon of {this_title}"
								else:
									voiceover_str += f"Coming up is a {this_title} marathon" 
							else:
								if voiceover_str:
									voiceover_str += f", followed by {repeats} episodes of {this_title}"
								else:
									voiceover_str += f"Coming up is {repeats} episodes of {this_title}"
						else:
							if this_record['is_marathon']:
								if voiceover_str:
									voiceover_str += f", then a {this_title} marathon"
								else:
									voiceover_str += f"Coming up is a {this_title} marathon" 
							else:
								if voiceover_str:
									voiceover_str += f", then {repeats} episodes of {this_title}"
								else:
									voiceover_str += f"Coming up is {repeats} episodes of {this_title}"
					else:
						## Last Record
						if len(remaining) <= i+1:
							if this_record['is_marathon']:
								if voiceover_str:
									voiceover_str += f", followed by a {this_title} marathon"
								else:
									voiceover_str += f"Coming up is a {this_title} marathon" 
							else:
								if voiceover_str:
									voiceover_str += f", followed by an episode of {this_title}"
								else:
									voiceover_str += f"Up next is an episode of {this_title}"
						else:
							if this_record['is_marathon']:
								if voiceover_str:
									voiceover_str += f", then a {this_title} marathon"
								else:
									if previous_schedule['is_marathon']:
										voiceover_str += f"Coming up we will continue our marathon of {this_title}"
									else:
										voiceover_str += f"Coming up is a {this_title} marathon" 
							else:
								if voiceover_str:
									voiceover_str += f", then an episode of {this_title}"
								else:
									voiceover_str += f"Up next is an episode of {this_title}"
				## Is a movie
				else:
					movie_name = remaining[i]['title']
					## Last record
					if len(remaining) <= i+1:
						if voiceover_str:
							voiceover_str += f", followed by {movie_name}"
						else:
							voiceover_str += f"Coming up is {movie_name}"
					else:
						if voiceover_str:
							voiceover_str += f", then {movie_name}"
						else:
							voiceover_str += f"Coming up is {movie_name}"
				i += 1

		cur.close()
		db.close()

		voiceover_str = "Time to stretch your legs, get a new snack and beer, and take a little break. But don't worry, there's more coming up on dumbie TV! " + voiceover_str.strip() + "."
		
		return voiceover_str
	
	def _generate_speech_pyt2s(self, schedule_id):
		_print(f"Generating speech for {schedule_id}", LOG_LEVEL_DEBUG)
		data = ibm_watson.requestTTS(
			self.generate_voiceover_text(schedule_id),
			ibm_watson.Voice.en_US_LisaExpressive.value
		)
		export_file = os.path.join(config.INTERMISSION_OUTPUT_PATH, "tts/", f"{schedule_id}.mp3")
		with open(export_file, "wb") as file:
			file.write(data)
		_print("Done!", LOG_LEVEL_DEBUG)
		return export_file

	def _generate_speech_gtts(self, schedule_id):
		tts = gTTS(text=self.generate_voiceover_text(schedule_id), lang="en", slow=False)
		tts.save(os.path.join(config.INTERMISSION_OUTPUT_PATH, f"{schedule_id}.mp3"))

	def _generate_audio_track(self, schedule_id, voiceover_file):
		_print(f"Generating audio track for {schedule_id}", LOG_LEVEL_DEBUG)
		background_audio_file = random.choice(os.listdir(os.path.join(config.INTERMISSION_RESOURCE_PATH, 'music/')))
		background_audio_file = os.path.join(config.INTERMISSION_RESOURCE_PATH, 'music/', background_audio_file)
		_print(f"Using background audio file {background_audio_file}", LOG_LEVEL_DEBUG)

		background_audio_type = background_audio_file.split('.')[-1]
		
		three_minutes = 3 * 60 * 1000
		background_audio = AudioSegment.from_file(background_audio_file, background_audio_type)[:three_minutes]

		vo_file_type = voiceover_file.split('.')[-1]
		if not os.path.exists(voiceover_file):
			_print(f'Voiceover audio file ({voiceover_file}) does not exist', LOG_LEVEL_ERROR)
			return False

		vo_audio = AudioSegment.from_file(voiceover_file, vo_file_type)

		vo_start = 5000
		vo_end = 5000 + len(vo_audio)
		while vo_end < len(background_audio):
			first_segment = background_audio[:vo_start]
			vo_segment = background_audio[vo_start:vo_end] - 16
			end_segment = background_audio[vo_end:]
			background_audio = first_segment + vo_segment + end_segment

			background_audio = background_audio.overlay(vo_audio, position=vo_start)
			vo_start = vo_start + (45*1000)
			vo_end = vo_end + (45*1000)

		export_file = os.path.join(config.INTERMISSION_OUTPUT_PATH, 'audio_tracks/', f'{schedule_id}.wav')
		background_audio.export(export_file, format="wav")
		_print("Done!", LOG_LEVEL_DEBUG)
		return export_file

	def _setup_output_dirs(self):
		if not os.path.exists(config.INTERMISSION_OUTPUT_PATH):
			_print(f'INTERMISSION_OUTPUT_PATH ({config.INTERMISSION_OUTPUT_PATH}) does not exist', LOG_LEVEL_ERROR)
			return
		
		for dir_name in ['tts', 'audio_tracks', 'complete']:
			if not os.path.exists(os.path.join(config.INTERMISSION_OUTPUT_PATH, dir_name)):
				os.makedirs(os.path.join(config.INTERMISSION_OUTPUT_PATH, dir_name))

	def _get_show_name(self, title_str, tts_clean=False):
		match = re.search(r'^(.+?)(\(.+\))?\s*(Marathon!)?\s*S(\d+) E(\d+)$', title_str)
		if match:
			name = match.group(1).strip()
		else:
			name = title_str
		
		if tts_clean:
			return name.replace(':', '')\
			.replace('!', '')
		else:
			return name