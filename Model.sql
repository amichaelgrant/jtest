CREATE TABLE location(
	advertiser_id varchar(255) NOT NULL,
	latitude double NOT NULL,
	longitude double NOT NULL,
	horizontal_accuracy double NOT NULL,
	timestamp datetime(6) NOT NULL,
	PRIMARY KEY (advertiser_id)
)ENGINE=InnoDB;
