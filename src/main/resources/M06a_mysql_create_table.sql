CREATE TABLE `M06A` (
	`VehicleType` VARCHAR(2) NOT NULL,
	`DetectionTime_O` DATETIME NOT NULL,
	`GantryID_O` VARCHAR(10) NOT NULL,
	`DetectionTime_D` DATETIME NOT NULL,
	`GantryID_D` VARCHAR(10) NOT NULL,
	`TripLength` FLOAT NOT NULL,
	`TripEnd` CHAR(1) NOT NULL,
	`TripInformation` TEXT NULL
)
COMMENT='各旅次路徑原始資料'
COLLATE='latin1_swedish_ci'
ENGINE=InnoDB
;

CREATE USER 'etc_user'@'%' IDENTIFIED BY 'etcuser1234';
ALTER USER 'etc_user'@'%' IDENTIFIED BY 'etcuser1234';

CREATE USER 'etc_user'@'localhost' IDENTIFIED BY 'etcuser1234';
GRANT ALL ON etc.* TO 'etc_user'@'localhost';

CREATE USER 'etc_user'@'%' IDENTIFIED BY '1qaz!QAZ';
GRANT ALL ON etc.* TO 'etc_user'@'%' IDENTIFIED BY '1qaz!QAZ';
FLUSH PRIVILEGES;

CREATE USER 'etc_user'@'localhost' IDENTIFIED BY '1qaz!QAZ';
GRANT ALL ON etc.* TO 'etc_user'@'localhost' IDENTIFIED BY '1qaz!QAZ';
FLUSH PRIVILEGES;