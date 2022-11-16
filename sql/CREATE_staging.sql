

CREATE TABLE ProvinceDIM(
	id int PRIMARY KEY,
	nameProvince NVARCHAR(100)
)

-- DROP TABLE provincedim
-- 
CREATE TABLE DateDIM(
	id int PRIMARY KEY,
	date text,
	year int,
	month int,
	day int,
	dayOfWeek VARCHAR(255)
)
-- drop table datedim

-- DROP table weatherfact
CREATE TABLE raw_weather_data(
	id VARCHAR(10) not null,
	province_name nvarchar(100) default null,
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
	CONSTRAINT CK_CHECK_SCOPE CHECK (
	humidity >=0.0 AND humidity <=100.0 AND vision >=0.0 AND wind >=0.0 AND
	stopPoint >=0 AND stopPoint <=100 AND uvIndex >=0.0 AND uvIndex <=100.0
	)
)

-- CREATE TABLE WeatherFact(
-- 	sk VARCHAR(10) PRIMARY KEY,
-- 	naturalKey VARCHAR(10),
-- 	provinceId VARCHAR(10),
-- 	dateId VARCHAR(10),
-- 	currentTemperature VARCHAR(255),
-- 	lowestTemperature VARCHAR(255),
-- 	overview VARCHAR(255),
-- 	wind VARCHAR(255),
-- 	vision VARCHAR(255),
-- 	stopPoint VARCHAR(255),
-- 	uvIndex int,
-- 	humidity float,
-- 	airQuality VARCHAR(255),
-- 	deleted int,
-- 	updated int,
-- 	timeExpried datetime DEFAULT '9999-12-31',
-- 
-- 	FOREIGN KEY (provinceId) REFERENCES ProvinceDIM(id),
-- 	FOREIGN KEY (dateId) REFERENCES DateDIM(id)
-- )