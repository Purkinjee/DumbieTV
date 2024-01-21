from datetime import datetime, timedelta, time as dttime
import time
import os, sys, signal
import subprocess
import threading
import queue

from lib.common import get_mysql_connection
import config

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
				print('Queue is empty')
				continue

			ffmpeg_params = [
				'ffmpeg',
				#'-hwaccel', 'cuda',
				'-hwaccel_output_format', 'cuda',
				'-re'
			]
			if to_play.get('skipto', None):
				ffmpeg_params += [
					'-ss', str(to_play['skipto'])
				]
			
			ffmpeg_params += [
				'-i', to_play['path'],
				'-c:v', 'h264_nvenc',
				#'-vf', "scale_cuda=w=1920:h=1080:force_original_aspect_ratio=0:format=yuv420p",
				'-vf', 'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1,format=yuv420p',
				'-pix_fmt', 'yuv420p',
				'-r', '30000/1001',
				'-c:a', 'aac',
				'-ar', '44100',
				'-b:a', "256k",
				'-ac', '1',
				'-f', 'flv',
				config.RTMP_POST
			]

			if to_play.get('wait_until', None) is not None:
				now = datetime.now()
				if to_play['wait_until'] > now:
					seconds_to_wait = (to_play['wait_until'] - now).seconds
					print(f"Thread was told to wait for {seconds_to_wait}s")
					time.sleep(seconds_to_wait)

			self.completed_queue.put({
				'id': to_play['id'],
				'start_time': datetime.now()
			})
			
			print(f"Playing {to_play['path']}")
			self._ffmpeg_process = subprocess.Popen(ffmpeg_params, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
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
			self._ffmpeg_process.terminate()

class Player:
	def __init__(self):
		self._db = get_mysql_connection()
	
	def close(self):
		self._db.close()
	
	def play(self):
		cur = self._db.cursor(dictionary=True)
		playlist_queue = queue.Queue()
		completed_queue = queue.Queue()

		q = "SELECT schedule.*, tv_episodes.path FROM schedule LEFT JOIN tv_episodes ON schedule.tv_episode_id = tv_episodes.id WHERE start_time <= NOW() AND end_time > NOW() ORDER BY start_time LIMIT 1"
		cur.execute(q)
		starting_schedule = cur.fetchone()

		if not starting_schedule:
			q = "SELECT * FROM schedule WHERE start_time >= NOW() ORDER BY start_time LIMIT 1"
			cur.execute(q)
			starting_schedule = cur.fetchone()
			if not starting_schedule:
				print("Nothing exists in schedule")
				return
			now = datetime.now()

			if now < starting_schedule['start_time']:
				to_wait = (starting_schedule['start_time'] - now).seconds
				print(f"Waiting for {to_wait}s for next scheduled show")
				time.sleep(to_wait)

		skipto = None
		if starting_schedule['start_time'] < datetime.now():
			gap = (datetime.now() - starting_schedule['start_time']).seconds
			if gap > 0:
				skipto = gap

		playlist_queue.put({
			'id': starting_schedule['id'],
			'path': starting_schedule['path'],
			'skipto': skipto,
		})

		pt = PlayerThread(playlist_queue, completed_queue)
		pt.start()

		previous_played = starting_schedule
		while True:
			try:
				if not completed_queue.empty():
					self._handle_completed(completed_queue)
				if not playlist_queue.empty():
					time.sleep(5)
					continue
				
				q = "SELECT schedule.*, tv_episodes.path FROM schedule LEFT JOIN tv_episodes ON schedule.tv_episode_id = tv_episodes.id WHERE start_time > %s ORDER BY start_time LIMIT 1"
				cur.execute(q, (previous_played['start_time'], ))
				next_schedule = cur.fetchone()

				if not next_schedule:
					print("Nothing in schedule")
					time.sleep(10)
					continue
				
				wait_until = None
				if next_schedule['start_time'] != previous_played['end_time']:
					wait_until = next_schedule['start_time']

				playlist_queue.put({
					'id': next_schedule['id'],
					'path': next_schedule['path'],
					'wait_until': wait_until
				})
				previous_played = next_schedule
			except KeyboardInterrupt:
				pt.stop()
				sys.exit(0)

	def _handle_completed(self, completed_queue):
		cur = self._db.cursor()
		while True:
			try:
				completed = completed_queue.get(block=False)
				if completed.get('start_time') is not None:
					q = "UPDATE schedule SET actual_start_time = %s, completed = 0 WHERE id = %s"
					cur.execute(q, (completed['start_time'], completed['id']))
				
				if completed.get('end_time') is not None:
					q = "UPDATE schedule SET actual_end_time = %s, completed = 1 WHERE id = %s"
					cur.execute(q, (
						completed['end_time'],
						completed['id']
					))
				self._db.commit()

			except queue.Empty:
				cur.close()
				return
