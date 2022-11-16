
CREATE TABLE SourceConfig(
	id VARCHAR(10) PRIMARY KEY,
	name nvarchar(150),
	url VARCHAR(255),
	pathFolder text,
	distFolder text
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
	sourceId VARCHAR(10),
	dateLoad date,
	timeLoad int,
	status VARCHAR(10)
)

