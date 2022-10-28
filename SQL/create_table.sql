CREATE TABLE if NOT EXISTS PUBLIC.cyclones (
 ID varchar(10),
 Name varchar(10),
 Date date,
 Time integer,
 Event varchar(5),
 Status varchar(2),
 Latitude varchar(10),
 Longitude varchar(10),
 MaximumWind integer,
 MinimumPressure integer,
 LowWindNE integer,
 LowWindSE integer,
 LowWindSW integer,
 LowWindNW integer,
 ModerateWindNE integer,
 ModerateWindSE integer,
 ModerateWindSW integer,
 ModerateWindNW integer,
 HighWindNE integer,
 HighWindSE integer,
 HighWindSW integer,
 HighWindNW integer);

CREATE TABLE if NOT EXISTS PUBLIC.cyclones_history(
from_date date,
end_date date,
ID varchar(10),
Status varchar(2));

CREATE TABLE if NOT EXISTS PUBLIC.cyclones_history_tmp(
id varchar(10),
date date,
status varchar(2));