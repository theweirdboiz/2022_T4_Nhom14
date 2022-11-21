CREATE TABLE timedim(
id int PRIMARY KEY,
hourMinuteValue VARCHAR(20)
)
DROP TABLE timedim

CREATE TABLE ProvinceDIM(
	id int PRIMARY KEY,
	nameProvince NVARCHAR(100)
)

-- DROP TABLE provincedim
-- 
CREATE TABLE DateDIM(
	id int PRIMARY KEY,
	date varchar(25),
	year int,
	month int,
	day int,
	dayOfWeek VARCHAR(25)
)
-- drop table datedim
-- DROP table raw_weather_data
CREATE TABLE raw_weather_data(
	id int NOT NULL AUTO_INCREMENT,
	province_name nvarchar(100) default null,
	date_load date default null,
	time_load time default null,
	currentTemp int default null,
	overview nvarchar(100) default null,
	lowestTemp int default null,
	maximumTemp int default null,
	humidity float default null,
	vision float default null,
	wind float default null, 
	stopPoint int default null, 
	uvIndex float default null,
	airQuality nvarchar(50) default null,
	PRIMARY KEY (id),
	CONSTRAINT CK_CHECK_SCOPE_1 CHECK (
	humidity >=0.0 AND humidity <=100.0 AND vision >=0.0 AND wind >=0.0 AND
	stopPoint >=0 AND stopPoint <=100 AND uvIndex >=0.0 AND uvIndex <=100.0
	)
)
CREATE TABLE raw_weather_data_thoitieteduvn(
	id VARCHAR(10) not null,
	province_name nvarchar(100) default null,
	date_load date default null,
	time_load int default null,
	currentTemp int default null,
	overview nvarchar(100) default null,
	lowestTemp int default null,
	maximumTemp int default null,
	humidity float default null,
	vision float default null,
	wind float default null, 
	stopPoint int default null, 
	uvIndex float default null,
	airQuality nvarchar(50) default null,
	PRIMARY KEY (id),
	CONSTRAINT CK_CHECK_SCOPE_2 CHECK (
	humidity >=0.0 AND humidity <=100.0 AND vision >=0.0 AND wind >=0.0 AND
	stopPoint >=0 AND stopPoint <=100 AND uvIndex >=0.0 AND uvIndex <=100.0
	)
)
drop table weatherfact
CREATE TABLE WeatherFact(
	sk int AUTO_INCREMENT,
	province_id int not null,
	date_id int not null,
	time_id int not null,
	currentTemp int,
	overview nvarchar(100),
	lowestTemp int,
	maximumTemp int,
	humidity float,
	vision float,
	wind float, 
	stopPoint int, 
	uvIndex float,
	airQuality nvarchar(50),
	loadTime time,
	expiredTime date default '9999-12-31',
	PRIMARY KEY (sk),
	FOREIGN KEY (province_id) REFERENCES provincedim(id),
	FOREIGN KEY (date_id) REFERENCES datedim(id),
	FOREIGN KEY (time_id) REFERENCES timedim(id)
	)
