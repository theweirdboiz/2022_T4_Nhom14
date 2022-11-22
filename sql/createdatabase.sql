-- Tao database control

CREATE DATABASE Control;

CREATE TABLE SourceConfig(

	id VARCHAR(10) PRIMARY KEY,
	name NVARCHAR(255),
	url VARCHAR(255),
	path_folder VARCHAR(255),
	dist_folder VARCHAR(255)

)

CREATE TABLE FtpConfig(

	id VARCHAR(10) PRIMARY KEY,
	host VARCHAR(255),
	port int,
	username VARCHAR(255),
	password VARCHAR(255),
	`using` bit
	
)

CREATE TABLE DbConfig(

	id VARCHAR(255) PRIMARY KEY,
	host VARCHAR(255),
	username VARCHAR(255),
	password VARCHAR(255),
	type VARCHAR(255),
	`using` bit

)

CREATE TABLE Log(

	id VARCHAR(10) PRIMARY KEY,
	source_id VARCHAR(10),
	time_load datetime,
	path_ftp VARCHAR(255),
	status VARCHAR(10)
	
)

-- tao database stagging

CREATE DATABASE Stagging;

drop table provincedim

CREATE TABLE ProvinceDIM(

	id VARCHAR(10) PRIMARY KEY,
	name_province VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci

)

CREATE TABLE DateDIM(

	id VARCHAR(10) PRIMARY KEY,
	date int,
	month int,
	year int,
	hour int,
	minute int,
	second int,
	day_of_week VARCHAR(255)

)

CREATE TABLE WeatherFact(

	id VARCHAR(10) PRIMARY KEY,
	province_id VARCHAR(10),
	date_id VARCHAR(10),
	current_temperature int,
	lowest_temperature int,
	highest_temperature int,
	humidity float,
	overview VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
	wind FLOAT,
	vision FLOAT,
	stop_point int,
	uv_index FLOAT,
	air_quality VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,

	FOREIGN KEY (province_id) REFERENCES ProvinceDIM(id),
	FOREIGN KEY (date_id) REFERENCES DateDIM(id)

)

-- tao database dataware house

CREATE DATABASE DatawareHouseWeather;

CREATE TABLE ProvinceDIM(

	id VARCHAR(10) PRIMARY KEY,
	name_province VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci

)

CREATE TABLE DateDIM(

	id VARCHAR(10) PRIMARY KEY,
	date int,
	month int,
	year int,
	hour int,
	minute int,
	second int,
	day_of_week VARCHAR(255)

)


CREATE TABLE WeatherFact(

	sk VARCHAR(10) PRIMARY KEY,
	natural_key VARCHAR(10),
	province_id VARCHAR(10),
	date_id VARCHAR(10),
	current_temperature int,
	lowest_temperature int,
	highest_temperature int,
	humidity float,
	overview VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
	wind FLOAT,
	vision FLOAT,
	stop_point int,
	uv_index FLOAT,
	air_quality VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
	deleted int,
	updated int,
	time_expried datetime DEFAULT '9999-12-31',

	FOREIGN KEY (province_id) REFERENCES ProvinceDIM(id),
	FOREIGN KEY (date_id) REFERENCES DateDIM(id)

)