CREATE TABLE tv_shows (
	id INT NOT NULL AUTO_INCREMENT,
	tvdb_id VARCHAR(50) NOT NULL,
	path VARCHAR(255) NOT NULL,
	title VARCHAR(255) NOT NULL,
	description TEXT DEFAULT NULL,
	thumbnail VARCHAR(255) DEFAULT NULL,
	thumbnail_width INT NOT NULL DEFAULT 0,
	thumnail_height INT NOT NULL DEFAULT 0,
	last_played_episode INT DEFAULT NULL,
	verified TINYINT NOT NULL DEFAULT 0,
	enabled TINYINT NOT NULL DEFAULT 0,
	needs_update TINYINT NOT NULL DEFAULT 0,
	last_updated DATETIME DEFAULT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE tv_episodes (
	id INT NOT NULL AUTO_INCREMENT,
	tv_show_id INT NOT NULL,
	tvdb_id VARCHAR(50),
	path VARCHAR(255) NOT NULL,
	duration INT NOT NULL,
	season_number INT NOT NULL,
	episode_number INT NOT NULL,
	description TEXT DEFAULT NULL,
	needs_update TINYINT NOT NULL DEFAULT 0,
	last_updated DATETIME DEFAULT NULL,
	transcoded TINYINT NOT NULL DEFAULT 0,
	PRIMARY KEY (id)
);

CREATE TABLE schedule (
	id INT NOT NULL AUTO_INCREMENT,
	tv_episode_id INT NOT NULL,
	start_time DATETIME NOT NULL,
	end_time DATETIME NOT NULL,
	actual_start_time DATETIME DEFAULT NULL,
	actual_end_time DATETIME DEFAULT NULL,
	completed TINYINT NOT NULL DEFAULT 0,
	PRIMARY KEY (id)
);