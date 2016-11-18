

DROP TABLE IF EXISTS beforeafter.intervention_periods ;
CREATE TABLE beforeafter.intervention_periods (
	intervention_id smallserial PRIMARY KEY,
	intervention_type_id smallint not null,
	route_id smallint not null, 
	segment_id smallint not null,
	period daterange not null,
	note varchar(256)
);


DROP TABLE IF EXISTS beforeafter.intervention_type;

CREATE TABLE beforeafter.intervention_type(
	intervention_type_id smallserial PRIMARY KEY,
	intervention varchar(32) NOT NULL UNIQUE
);

DROP TABLE IF EXISTS beforeafter.routes;

CREATE TABLE beforeafter.routes (
	route_id smallserial PRIMARY KEY,
	route_name varchar(36) not null UNIQUE
);

DROP TABLE IF EXISTS beforeafter.segments;

CREATE TABLE beforeafter.segments (
	segment_id smallserial PRIMARY KEY,
	segment_name varchar(64) not null UNIQUE
);

DROP TABLE IF EXISTS beforeafter.segment_tmcs;
CREATE TABLE beforeafter.segment_tmcs(
	segment_id smallint not null,
	tmc char(9) NOT NULL,
	PRIMARY KEY(segment_id, tmc)
);

DROP TABLE IF EXISTS beforeafter.signal_retiming_staging;
CREATE TABLE beforeafter.signal_retiming_staging(
	route_name varchar(36) not null,
	segment_name varchar(64) not null,
	period_text varchar(64) not null,
	note varchar(256)
);