
CREATE TABLE SourceConfig(
	id VARCHAR(10) PRIMARY KEY,
	name nvarchar(150),
	url VARCHAR(255),
	pathFolder text,
	distFolder text,
	ftp_id int,
	FOREIGN KEY (ftp_id) REFERENCES ftpconfig(id)
)

CREATE TABLE FtpConfig(
	id int PRIMARY KEY,
	host VARCHAR(255),
	port int,
	username VARCHAR(255),
	password VARCHAR(255)
)
-- drop table dbconfig
CREATE TABLE DbConfig(
	id int PRIMARY KEY,
	driver varchar(20),
	location VARCHAR(75),
	username VARCHAR(100),
	password VARCHAR(50),
	dbName VARCHAR(100)
)

CREATE TABLE Log(
	id int PRIMARY KEY,
	sourceId VARCHAR(10),
	dateLoad date,
	timeLoad int,
	status VARCHAR(10)
)

