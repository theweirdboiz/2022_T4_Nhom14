-- Tao database control

CREATE DATABASE Control;

CREATE TABLE SourceConfig(

	id VARCHAR(10) PRIMARY KEY,
	name NVARCHAR(255),
	url VARCHAR(255),
	pathFolder VARCHAR(255),
	distFolder VARCHAR(255)

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

DROP TABLE log

CREATE TABLE Log(

	id VARCHAR(10) PRIMARY KEY,
	sourceId VARCHAR(10),
	timeLoad datetime,
	pathFTP VARCHAR(255),
	status VARCHAR(10)
	
)

-- tao database stagging

CREATE DATABASE Stagging;

drop table provincedim

CREATE TABLE ProvinceDIM(

	id VARCHAR(10) PRIMARY KEY,
	nameProvince VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci

)

CREATE TABLE DateDIM(

	id VARCHAR(10) PRIMARY KEY,
	date int,
	month int,
	year int,
	hour int,
	minute int,
	second int,
	dayOfWeek VARCHAR(255)

)

CREATE TABLE WeatherFact(

	id VARCHAR(10) PRIMARY KEY,
	provinceId VARCHAR(10),
	dateId VARCHAR(10),
	currentTemperature int,
	lowestTemperature int,
	highestTemperature int,
	humidity float,
	overview VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
	wind FLOAT,
	vision FLOAT,
	stopPoint int,
	uvIndex FLOAT,
	airQuality VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,

	FOREIGN KEY (provinceId) REFERENCES ProvinceDIM(id),
	FOREIGN KEY (dateId) REFERENCES DateDIM(id)

)

-- tao database dataware house

CREATE DATABASE DatawareHouseWeather;

CREATE TABLE ProvinceDIM(

	id VARCHAR(10) PRIMARY KEY,
	nameProvince VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci

)

CREATE TABLE DateDIM(

	id VARCHAR(10) PRIMARY KEY,
	date int,
	month int,
	year int,
	hour int,
	minute int,
	second int,
	dayOfWeek VARCHAR(255)

)


CREATE TABLE WeatherFact(

	sk VARCHAR(10) PRIMARY KEY,
	naturalKey VARCHAR(10),
	provinceId VARCHAR(10),
	dateId VARCHAR(10),
	currentTemperature int,
	lowestTemperature int,
	highestTemperature int,
	humidity float,
	overview VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
	wind FLOAT,
	vision FLOAT,
	stopPoint int,
	uvIndex FLOAT,
	airQuality VARCHAR(255)CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
	deleted int,
	updated int,
	timeExpried datetime DEFAULT '9999-12-31',

	FOREIGN KEY (provinceId) REFERENCES ProvinceDIM(id),
	FOREIGN KEY (dateId) REFERENCES DateDIM(id)

)