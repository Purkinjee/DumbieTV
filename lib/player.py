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

			stats = {
				'id': to_play['id'],
				'start_time': datetime.now()
			}
				
			#self._ffmpeg_process = subprocess.Popen(ffmpeg_params, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
			self._ffmpeg_process = subprocess.Popen(ffmpeg_params)
			while self._ffmpeg_process.poll() is None:
				time.sleep(1)

			print(to_play['path'])
			print(' '.join(ffmpeg_params))
			stats['end_time'] = datetime.now()

			self._ffmpeg_process = None
			if self._keep_listening:
				self.completed_queue.put(stats)
		
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
				print("Video Complete")
				print(completed)
				q = "UPDATE schedule SET actual_start_time = %s, actual_end_time = %s, completed = 1 WHERE id = %s"
				cur.execute(q, (
					completed['start_time'],
					completed['end_time'],
					completed['id']
				))
				self._db.commit()

			except queue.Empty:
				cur.close()
				return

	def play_old2(self):

		while True:
			cur = self._db.cursor(dictionary=True)
			q = "SELECT * FROM schedule WHERE start_time <= NOW() AND end_time > NOW() ORDER BY start_time LIMIT 1"
			cur.execute(q)
			starting_schedule = cur.fetchone()

			if not starting_schedule:
				q = "SELECT * FROM schedule WHERE start_time >= NOW() ORDER BY start_time LIMIT 1"
				cur.execute(q)
				next_episode = cur.fetchone()

				if not next_episode:
					print("No future schedule exists. Exiting")
					break
				
				time_until_next = (next_episode['start_time'] - datetime.now()).seconds
				print(f"Nothing to play. Sleeping for {time_until_next} seconds")
				time.sleep(time_until_next)
				continue
			
			if starting_schedule['start_time'].hour >= 4:
				end_time = datetime.combine(datetime.now().date() + timedelta(days=1), dttime(4))
			else:
				end_time = datetime.combine(datetime.now().date(), dttime(4))

			q = "SELECT * FROM schedule WHERE start_time >= %s AND end_time <= %s ORDER BY start_time"
			cur.execute(q, (starting_schedule['start_time'], end_time))
			todays_schedule = cur.fetchall()

			ffmpeg_params = ['ffmpeg', '-re']

			if todays_schedule[0]['start_time'] < datetime.now():
				delay = (datetime.now() - todays_schedule[0]['start_time']).seconds
				if delay > 0:
					ffmpeg_params += ['-ss', str(delay)]

			ffmpeg_params += ['-f', 'concat', '-safe', '0']
			playlist_file = os.path.join(config.TRANSCODED_VIDEO_LOCATION, datetime.now().strftime("%Y%m%d.txt"))
			if os.path.exists(playlist_file):
				os.remove(playlist_file)

			with open(playlist_file, 'w+') as f:
				for video in todays_schedule:
					transcoded_video_file = os.path.join(config.TRANSCODED_VIDEO_LOCATION, 'transcoded/', f"{video['tv_episode_id']}.mp4")
					if not os.path.exists(transcoded_video_file):
						print(f"Cannot find video!!! {transcoded_video_file}")
						continue
					f.write(f"file '{transcoded_video_file}'\n")
			
			ffmpeg_params += [
				#'-hwaccel', 'cuda',
				#'-hwaccel_output_format', 'cuda',
				'-i', playlist_file,
				#'-c:v', 'h264_nvenc',
				'-c:v', 'copy',
				'-c:a', 'aac',
				'-tune', 'zerolatency',
				'-ar', '44100',
				'-b:a', '256k',
				'-ac', '1',
				'-f', 'flv',
				config.RTMP_POST
			]

			cur.close()
			#process = subprocess.Popen(ffmpeg_params, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
			process = subprocess.Popen(ffmpeg_params, stderr=subprocess.STDOUT)
			return_code = process.wait()


	def play_old(self):
		cur = self._db.cursor(dictionary=True)

		q = "SELECT * FROM schedule WHERE start_time <= NOW() AND end_time > NOW() ORDER BY start_time LIMIT 1"
		cur.execute(q)
		starting_schedule = cur.fetchone()

		if not starting_schedule:
			q = "SELECT * FROM schedule WHERE start_time >= NOW() ORDER BY start_time LIMIT 1"
			cur.execute(q)
			starting_schedule = cur.fetchone()

		if not starting_schedule:
			print("Nothing to play")
			return
		
		offset = None
		if starting_schedule['start_time'] < datetime.now():
			offset = (datetime.now() - starting_schedule['start_time']).seconds
		currently_streaming = starting_schedule
		while True:
			time_until_start = (currently_streaming['start_time'] - datetime.now()).seconds
			
			while time_until_start > 5 and currently_streaming['start_time'] > datetime.now():
				print("Not time for next scheduled video yet. Waiting...")
				time.sleep(time_until_start / 2)
				time_until_start = (currently_streaming['start_time'] - datetime.now()).seconds
			
			self._stream_scheduled_video(currently_streaming['id'], offset)
			q = "SELECT * FROM schedule WHERE start_time > %s AND completed = 0 ORDER BY start_time LIMIT 1"
			cur.execute(q, (currently_streaming['start_time'], ))
			currently_streaming = cur.fetchone()
			offset = None

			if not currently_streaming:
				print("Nothing else on schedule to stream")
				break

		cur.close()

	def _stream_scheduled_video(self, schedule_id, offset=None):
		cur = self._db.cursor(dictionary=True)

		q = "SELECT schedule.id, schedule.tv_episode_id, schedule.actual_start_time, tv_episodes.path FROM schedule LEFT JOIN tv_episodes ON schedule.tv_episode_id = tv_episodes.id  WHERE schedule.id = %s"
		cur.execute(q, (schedule_id, ))
		res = cur.fetchone()

		if res['actual_start_time'] is None:
			q = "UPDATE schedule SET actual_start_time = %s WHERE id = %s"
			cur.execute(q, (datetime.now(), res['id']))
			self._db.commit()

		ffmpeg_params = ['ffmpeg']
		if offset is not None:
			ffmpeg_params += ['-ss', str(offset)]

		ffmpeg_params += [
			'-re',
			'-i', res['path'],
			'-c:v', 'copy',
			'-c:a', 'aac',
			'-ar', '44100',
			'-ac', '1',
			'-f', 'flv',
			config.RTMP_POST
		]

		#process = subprocess.Popen(ffmpeg_params, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
		process = subprocess.Popen(ffmpeg_params, stderr=subprocess.STDOUT)
		return_code = process.wait()

		q = "UPDATE schedule SET actual_end_time = %s, completed = 1 WHERE id = %s"
		cur.execute(q, (datetime.now(), schedule_id))
		self._db.commit()

		cur.close()