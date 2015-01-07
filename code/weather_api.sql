/**************************************************
   Data Wranglers DC : January 2015 Meetup
   JSON Processing in the Database
   ************************************************
   Author:  Ryan B. Harvey
   Created: January 4, 2015
   ************************************************
   This script demonstrates pulling data from the
   OpenWeatherMap API directly into a PostgreSQL 
   9.4 table, then manipulating the data into 
   tabular form suitable for future analysis.

   API Docs: http://openweathermap.org/api
***************************************************/

/* Setup code */
create schema if not exists weather_api;
set search_path to weather_api;

/* Cleanup from previous work */
/*
drop view if exists weather_analyis cascade;
drop table if exists weather_forecasts cascade;
drop table if exists weather_forecasts_xref;
drop table if exists weather_measures;
drop table if exists weather_api_data;
drop table if exists cities;
*/

/* We'll start by creating a temp table to dump API data into.
   Note the default values set for the non-JSON fields.
   These allow you to automatically timestamp and userstamp the
   data as it comes into the table.
 */
drop table if exists weather_api_data_raw;
create table weather_api_data_raw (
  response jsonb,
  captured_at timestamp with time zone 
              not null 
              default current_timestamp,
  captured_by character varying 
              not null 
              default current_user
);

/* Now, we'll pull data directly (via cURL) from the OpenWeatherMap API into the response column.
   Here, the API calls pull 1000 weather records each surrounding the following locations: 
     - New Orleans, LA, USA (Latitude 30.03, Longitude -90.03)
     - Moscow, Russia (Latitude 55.75, Longitude 37.62)
     - Lhasa, Tibet, China (Latitude 29.39, Longitude 91.07)
     - Sendai, Japan (Latitude 38.27, Longitude 140.87)
     - Cape Town, South Africa (Latitude -33.92, Longitude 18.42)
   NOTE: This step takes a minute or two...
 */
/* New Orleans, LA, USA */
copy weather_api_data_raw (response)
from
  program 'curl "http://api.openweathermap.org/data/2.5/find?lat=30.03&lon=-90.03&cnt=1000"';
/* Moscow, Russia */
copy weather_api_data_raw (response)
from 
  program 'curl "http://api.openweathermap.org/data/2.5/find?lat=55.75&lon=37.62&cnt=1000"';
/* Lhasa, Tibet, China */
copy weather_api_data_raw (response)
from
  program 'curl "http://api.openweathermap.org/data/2.5/find?lat=29.39&lon=91.07&cnt=1000"'; 
/* Sendai, Japan */
copy weather_api_data_raw (response)
from
  program 'curl "http://api.openweathermap.org/data/2.5/find?lat=38.27&lon=140.87&cnt=1000"';
/* Cape Town, South Africa */
copy weather_api_data_raw (response)
from
  program 'curl "http://api.openweathermap.org/data/2.5/find?lat=-33.92&lon=18.42&cnt=1000"';

/* Let's see what we've got */
select * from weather_api_data_raw;

/* Looks like each pull here is a rather complicated object.  Let's see what's there... */
select distinct jsonb_object_keys(response)
from weather_api_data_raw;

select (jsonb_each(response)).key, (jsonb_each(response)).value
from (select response from weather_api_data_raw limit 1) a;

/* Seems as if the interesting stuff is in the object attribute 'list'. 
   Let's split the records for each city into a table.
 */
create table if not exists weather_api_data (
  id serial primary key,
  city bigint,
  city_data jsonb,
  captured_at timestamp with time zone,
  captured_by character varying
);

insert into weather_api_data (city_data, captured_at, captured_by)
select jsonb_array_elements(response->'list'), captured_at, captured_by
from weather_api_data_raw;

select * from weather_api_data;

/* Cleanup: drop the weather_api_data_raw temp table */
-- drop table weather_api_data_raw;

/* Now, let's see what's in each of the objects in 'list', now in our city_data column */
select distinct jsonb_object_keys(city_data)
from weather_api_data;

select (jsonb_each(city_data)).key, (jsonb_each(city_data)).value
from (select city_data from weather_api_data limit 1) a;

/* Looks like we can pull the API's ID for the city, the city name, and the latitude and 
   longitude for each pretty easily.  Let's put that information into a new cities table.
 */
create table cities (
  id serial primary key,
  city_name text,
  api_id bigint,
  lat double precision,
  lon double precision,
  country text,
  captured_at timestamp with time zone,
  captured_by character varying
);

select
  city_data->>'name' as city_name,
  (city_data->>'id')::bigint as api_id,
  (city_data#>>'{coord,lat}')::double precision as lat,
  (city_data#>>'{coord,lon}')::double precision as lon,
  city_data#>>'{sys,country}' as country,
  captured_at, captured_by
from weather_api_data;

/* As an aside -- there's another way to get the same data via functions. */
select
  city_data->>'name' as city_name,
  jsonb_extract_path_text(city_data,'id')::bigint as api_id,
  jsonb_extract_path_text(city_data,'coord','lat')::double precision as lat,
  jsonb_extract_path_text(city_data,'coord','lon')::double precision as lon,
  jsonb_extract_path_text(city_data,'sys','country') as country,
  captured_at, captured_by
from weather_api_data;

/* Looks good, let's insert this into the table. */
insert into cities (city_name, api_id, lat, lon, country, captured_at, captured_by)
select
  city_data->>'name' as city_name,
  jsonb_extract_path_text(city_data,'id')::bigint as api_id,
  jsonb_extract_path_text(city_data,'coord','lat')::double precision as lat,
  jsonb_extract_path_text(city_data,'coord','lon')::double precision as lon,
  jsonb_extract_path_text(city_data,'sys','country') as country,
  captured_at, captured_by
from weather_api_data;

select * from cities;

/* Now we'd like to make a real relational dataset -- meaning we need to link tables 
   together via foreign keys.  Let's do that with cities and their weather data.
 */
alter table weather_api_data
add foreign key (city) 
  references cities (id);

/* We can also add an index to aid query performance. */
drop index if exists idx_jsonb_weather_data_city_data;
create index idx_jsonb_weather_data_city_data
  on weather_api_data using gin (city_data);

/* Now, let's update the foreign key field. */
update weather_api_data
set city = c.id
from cities c
where city_data->>'name' = c.city_name
  and jsonb_extract_path_text(city_data, 'coord', 'lat')::double precision = c.lat
  and jsonb_extract_path_text(city_data, 'coord', 'lon')::double precision = c.lon;

select * from weather_api_data;

/* Now, let's look back at what we have. */
select (jsonb_each(city_data)).key, (jsonb_each(city_data)).value
from (select city_data from weather_api_data limit 1) a;

/* The 'main' object looks interesting.  Let's see what's in there. */
select (jsonb_each(main)).key, (jsonb_each(main)).value
from (select city_data->'main' as main from weather_api_data limit 1) a;

/* Let's make a table for that and link it to our cities table. */
create table if not exists  weather_measures (
  id serial,
  city_id bigint,
  weather_api_data_id bigint,
  temperature float,
  pressure float,
  humidity int,
  temp_min float,
  temp_max float,
  sea_level float,
  ground_level float,
  foreign key (city_id) references cities (id)
);

insert into weather_measures (city_id, weather_api_data_id, temperature, pressure,
                              humidity, temp_min, temp_max, sea_level, ground_level)
select 
  c.id as city_id, w.id as weather_api_data_id,
  (w.city_data#>>'{main,temp}')::float as temperature,
  (w.city_data#>>'{main,pressure}')::float as pressure,
  (w.city_data#>>'{main,humidity}')::int as humidity,
  (w.city_data#>>'{main,temp_min}')::float as temp_min,
  (w.city_data#>>'{main,temp_max}')::float as temp_max,
  (w.city_data#>>'{main,sea_level}')::float as sea_level,
  (w.city_data#>>'{main,grnd_level}')::float as ground_level
from cities c inner join weather_api_data w on c.id = w.city;

/* Now, we can build a table from that and our cities table. */
select 
  c.city_name, c.lat, c.lon, m.temperature, m.pressure, m.humidity, 
  m.temp_min, m.temp_max, m.sea_level, m.ground_level, m.city_id, 
  m.weather_api_data_id 
from weather_measures m 
  inner join cities c on c.id = m.city_id
order by round(c.lat), c.city_name;

/* Let's see what else is in this object. */
select (jsonb_each(city_data)).key, (jsonb_each(city_data)).value
from (select city_data from weather_api_data limit 1) a;

/* Let's add wind and cloudiness to this table. */
alter table weather_measures
add column wind_deg float,
add column wind_speed float,
add column cloudiness_pct integer;

update weather_measures
set wind_deg = (w.city_data#>>'{wind,deg}')::float,
    wind_speed = (w.city_data#>>'{wind,speed}')::float,
    cloudiness_pct = (w.city_data#>>'{clouds,all}')::integer
from weather_api_data w
where weather_measures.weather_api_data_id = w.id;

select * from weather_measures;

/* Let's see what else is in this object. */
select (jsonb_each(city_data)).key, (jsonb_each(city_data)).value
from (select city_data from weather_api_data limit 1) a;

/* Now, let's add the weather forecasts in their own table, linked to the cities table
 * via a cross-reference table.
 * Note that the "icon" field identifies the icon file as 'http://openweathermap.org/img/w/---.png'
 */
create table if not exists weather_forecasts (
  id integer,
  icon_id text,
  icon_url text,
  category text,
  description text,
  primary key (id, icon_id)
);

insert into weather_forecasts (id, icon_id, icon_url, category, description)
with unique_forecasts (weather_forecast) as (
  select distinct 
    jsonb_array_elements(city_data->'weather') as weather_forecast
  from weather_api_data
)
select 
  (weather_forecast->>'id')::integer as id,
  weather_forecast->>'icon' as icon_id,
  'http://openweathermap.org/img/w/' || 
      jsonb_extract_path_text(weather_forecast,'icon') || 
      '.png' as icon_url,
  weather_forecast->>'main' as category,
  weather_forecast->>'description' as description
from unique_forecasts;

select * from weather_forecasts;

/* Now, we can link up the forecasts with the other weather data. */
create table weather_forecasts_xref (
  id serial primary key,
  city_id bigint,
  weather_forecast_id integer
);

select 
  city as city_id,
  (jsonb_array_elements(city_data->'weather')->>'id')::integer as weather_forecast_id
from weather_api_data;

insert into weather_forecasts_xref (city_id, weather_forecast_id)
select 
  city as city_id,
  (jsonb_array_elements(city_data->'weather')->>'id')::integer as weather_forecast_id
from weather_api_data;

select * from weather_forecasts_xref;

select * 
from cities c inner join weather_forecasts_xref x on c.id = x.city_id
inner join weather_forecasts f on x.weather_forecast_id = f.id;

/* Finally, for good measure, let's add that timestamp field 'dt' to weather_api_data.
 * Note that it happens to be in UNIX epoch time, so we'll need the to_timestamp() function.
 */
select to_timestamp((city_data->>'dt')::bigint) from weather_api_data;

alter table weather_api_data
add column ts timestamp with time zone;

update weather_api_data
set ts = to_timestamp((city_data->>'dt')::bigint);

select * from weather_api_data;

/* Now, for a huge analysis table */
create view weather_analysis as (
  select 
    w.captured_at, w.captured_by, ts as forecast_timestamp,
    c.id as city_id, c.city_name, m.temperature, m.pressure,
    m.humidity, m.temp_min, m.temp_max, m.sea_level, m.ground_level,
    m.wind_deg, m.wind_speed, m.cloudiness_pct, c.api_id as api_city_id,
    c.lat as latitude, c.lon as longitude, f.category as forecast, 
    f.description as forecast_description, f.icon_url as forecast_icon_url
  from weather_api_data w
    left join weather_measures m on w.id = m.weather_api_data_id
    left join cities c on w.city = c.id
    left join weather_forecasts_xref x on c.id = x.city_id
    left join weather_forecasts f on x.weather_forecast_id = f.id
);

select * from weather_analysis;

select * from weather_analysis where city_name = 'Sendai-shi';
select * from weather_analysis where city_name = 'New Orleans';
