from datetime import datetime, timedelta, time as dttime
import time
import os, sys, signal
import subprocess
import threading
import queue
import json

from lib.common import get_mysql_connection, Logger
from lib.vars import *
import config

_print = Logger()._print

class PlayerThread(threading.Thread):
	def __init__(self, playlist_queue, completed_queue):
		threading.Thread.__init__(self)

		self.playlist_queue = playlist_queue
		self.completed_queue = completed_queue

		self._keep_listening = True
		self._ffmpeg_process = None
	
	def run(self):
		while self._keep_listening:
			try:
				to_play = self.playlist_queue.get(timeout=5)
			except queue.Empty:
				_print('Queue is empty', LOG_LEVEL_ERROR)
				continue

			if to_play.get('schedule_start_time', None) is not None:
				_print(f"Scheduled start time: {to_play['schedule_start_time']}", LOG_LEVEL_DEBUG)
				offset = (to_play['schedule_start_time'] - datetime.now()).total_seconds()
				_print(f"Offset is {offset}s", LOG_LEVEL_DEBUG)
				if offset > 3600:
					_print(f"Scheduled time more than 1 hour in the future ({offset}s)", LOG_LEVEL_ERROR)
					_print("Something is likely wrong", LOG_LEVEL_ERROR)
					_print("Waiting until scheduled time to resume", LOG_LEVEL_ERROR)
					to_play['wait_until'] = to_play['schedule_start_time']
			

			ffmpeg_params = [
				config.FFMPEG_PATH,
				'-hwaccel_output_format', 'cuda',
				'-re'
			]
			if to_play.get('skipto', None):
				ffmpeg_params += [
					'-ss', str(to_play['skipto'])
				]
			
			ffmpeg_params += ['-i', to_play['path']]

			filters = (
				f'[0:v:{to_play.get("video_track", "0")}]scale=1920:1080:force_original_aspect_ratio=decrease[v],'
				'[v]pad=1920:1080:(ow-iw)/2:(oh-ih)/2[v],'
				'[v]setsar=1[v],'
				'[v]format=yuv420p[v]'
			)

			if config.WATERMARK:
				ffmpeg_params += ['-i', config.WATERMARK]
				filters += (
					',[1:v]format=rgba,colorchannelmixer=aa=0.5[overlay],'
					'[v][overlay]overlay=(main_w-overlay_w)-30:(main_h-overlay_h)-30[v]'
				)
			ffmpeg_params += [
				'-c:v', 'h264_nvenc',
				'-filter_complex', filters,
				'-pix_fmt', 'yuv420p',
				'-r', '30000/1001',
				'-c:a', 'aac',
				'-ar', '44100',
				'-b:a', "256k",
				'-ac', "1",
				'-map', '[v]',
				'-map', f"0:{to_play.get('audio_track', '1')}",
				'-f', 'flv',
				config.RTMP_POST
			]

			if to_play.get('wait_until', None) is not None:
				now = datetime.now()
				if to_play['wait_until'] > now:
					seconds_to_wait = (to_play['wait_until'] - now).total_seconds()
					_print(f"Thread was told to wait for {seconds_to_wait}s", LOG_LEVEL_INFO)
					time.sleep(seconds_to_wait)

			self.completed_queue.put({
				'id': to_play['id'],
				'start_time': datetime.now()
			})
			
			_print(f"Playing {to_play['path']}", LOG_LEVEL_INFO)
			self._ffmpeg_process = subprocess.Popen(
				ffmpeg_params, 
				stdout=subprocess.DEVNULL, 
				stderr=subprocess.STDOUT
			)
			#self._ffmpeg_process = subprocess.Popen(ffmpeg_params)
			while self._ffmpeg_process.poll() is None:
				time.sleep(1)

			self._ffmpeg_process = None
			if self._keep_listening:
				self.completed_queue.put({
					'id': to_play['id'],
					'end_time': datetime.now()
				})
		
	def stop(self):
		self._keep_listening = False
		if self._ffmpeg_process is not None:
			_print("Terminating ffmpeg process", LOG_LEVEL_DEBUG)
			self._ffmpeg_process.terminate()

class Player:
	def __init__(self, logger=None):
		if logger is not None:
			global _print
			_print = logger._print
	
	def close(self):
		pass
	
	def play(self):
		db = get_mysql_connection()
		cur = db.cursor(dictionary=True)
		playlist_queue = queue.Queue()
		completed_queue = queue.Queue()

		q = (
			"SELECT * FROM schedule "
			"WHERE start_time <= NOW() "
			"AND end_time > NOW() "
			"AND path IS NOT NULL "
			"ORDER BY start_time "
			"LIMIT 1"
		)
		cur.execute(q)
		starting_schedule = cur.fetchone()

		if not starting_schedule:
			q = (
				"SELECT * FROM schedule "
				"WHERE start_time >= NOW() "
				"AND path IS NOT NULL "
				"ORDER BY start_time "
				"LIMIT 1"
			)
			cur.execute(q)
			starting_schedule = cur.fetchone()
			if not starting_schedule:
				_print("Nothing exists in schedule", LOG_LEVEL_ERROR)
				return
			now = datetime.now()

			if now < starting_schedule['start_time']:
				to_wait = (starting_schedule['start_time'] - now).total_seconds()
				_print(f"Waiting for {to_wait}s for next scheduled show", LOG_LEVEL_INFO)
				time.sleep(to_wait)

		skipto = None
		if starting_schedule['start_time'] < datetime.now():
			gap = (datetime.now() - starting_schedule['start_time']).total_seconds()
			if gap > 0:
				skipto = gap

		_print(f"Skipping {skipto}s of first show", LOG_LEVEL_DEBUG)

		playlist_queue.put({
			'id': starting_schedule['id'],
			'path': starting_schedule['path'],
			'schedule_start_time': starting_schedule['start_time'],
			'skipto': skipto,
			'audio_track': self._get_audio_track(starting_schedule['path']),
			'video_track': self._get_video_track(starting_schedule['path'])
		})

		pt = PlayerThread(playlist_queue, completed_queue)
		pt.start()

		previous_played = starting_schedule
		cur.close()
		db.close()
		while True:
			try:
				if not completed_queue.empty():
					self._handle_completed(completed_queue)
				if not playlist_queue.empty():
					time.sleep(5)
					continue
				
				db = get_mysql_connection()
				cur = db.cursor(dictionary=True)
				
				q = (
					"SELECT * FROM schedule "
					"WHERE start_time > "
						"(SELECT start_time FROM schedule WHERE id = %s) "
					"AND path IS NOT NULL "
					"ORDER BY start_time "
					"LIMIT 1"
				)
				cur.execute(q, (previous_played['id'], ))
				next_schedule = cur.fetchone()

				if not next_schedule:
					_print("Nothing in schedule", LOG_LEVEL_ERROR)
					time.sleep(10)
					continue
				
				wait_until = None
				if next_schedule['start_time'] != previous_played['end_time']:
					wait_until = next_schedule['start_time']

				_print(f"Adding {next_schedule['path']} to the queue", LOG_LEVEL_DEBUG)
				playlist_queue.put({
					'id': next_schedule['id'],
					'path': next_schedule['path'],
					'schedule_start_time': next_schedule['start_time'],
					'wait_until': wait_until,
					'audio_track': self._get_audio_track(next_schedule['path']),
					'video_track': self._get_video_track(next_schedule['path'])
				})
				previous_played = next_schedule

				cur.close()
				db.close()
			except KeyboardInterrupt:
				pt.stop()
				sys.exit(0)

	def _get_audio_track(self, file_path):
		ffprobe_params = [
			config.FFPROBE_PATH,
			'-hide_banner', '-show_streams',
			'-print_format', 'json',
			file_path
		]
		process = subprocess.Popen(
			ffprobe_params, 
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE
		)

		#process.wait()
		data, err = process.communicate()
		#print(data)
		audio_tracks = []
		preferred_lang_tracks = []
		if process.returncode == 0:
			output = json.loads(data.decode('utf-8'))

			for stream in output.get('streams', []):
				if stream.get('codec_type') != 'audio':
					continue
				audio_tracks.append(stream['index'])
				if stream.get('tags', {}).get('language', '').lower() == config.AUDIO_LANG:
					if stream.get('codec_name') in ['dts', 'ac3', 'aac']:
						_print(f"Found {stream['codec_name']} {config.AUDIO_LANG} audio track for {file_path} ({stream['index']})", LOG_LEVEL_DEBUG)
						return stream['index']
					preferred_lang_tracks.append(stream)
		
		if len(preferred_lang_tracks) > 0:
			_print("No ac3/aac audio track found. Using first track for preferred language:", LOG_LEVEL_DEBUG)
			_print(f"  codec: {preferred_lang_tracks[0]['codec_name']} track: {preferred_lang_tracks[0]['index']}", LOG_LEVEL_DEBUG)
			return preferred_lang_tracks[0]['index']
		
		if len(audio_tracks) > 0:
			_print(f"No audio track found tagged {config.AUDIO_LANG} for {file_path}. Using {audio_tracks[0]}", LOG_LEVEL_DEBUG)
			return audio_tracks[0]
		
		_print(f"No audio track found for {file_path}", LOG_LEVEL_ERROR)
		return 1

	def _get_video_track(self, file_path):
		ffprobe_params = [
			config.FFPROBE_PATH,
			'-hide_banner', '-show_streams',
			'-print_format', 'json',
			file_path
		]
		process = subprocess.Popen(
			ffprobe_params, 
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE
		)

		#process.wait()
		data, err = process.communicate()
		if process.returncode == 0:
			output = json.loads(data.decode('utf-8'))

			for stream in output.get('streams', []):
				if stream.get('codec_type') == 'video':
					_print(f"Found video track for {file_path} ({stream['index']})", LOG_LEVEL_DEBUG)
					return stream['index']
		
		_print(f"No video track found for {file_path}", LOG_LEVEL_ERROR)
		return 0

	def _handle_completed(self, completed_queue):
		db = get_mysql_connection()
		cur = db.cursor()
		while True:
			try:
				completed = completed_queue.get(block=False)
				if completed.get('start_time') is not None:
					_print(f"Setting start time to {completed['start_time']} for {completed['id']}", LOG_LEVEL_DEBUG)
					q = (
						"UPDATE schedule "
						"SET actual_start_time = %s, "
						"completed = 0 "
						"WHERE id = %s"
					)
					cur.execute(q, (completed['start_time'], completed['id']))
				
				if completed.get('end_time') is not None:
					_print(f"Setting end time to {completed['end_time']} for {completed['id']}", LOG_LEVEL_DEBUG)
					q = (
						"UPDATE schedule "
						"SET actual_end_time = %s, "
						"completed = 1 "
						"WHERE id = %s"
					)
					cur.execute(q, (
						completed['end_time'],
						completed['id']
					))
				db.commit()

			except queue.Empty:
				cur.close()
				db.close()
				return
