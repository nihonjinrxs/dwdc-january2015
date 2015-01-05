/**************************************************
   Data Wranglers DC : January 2015 Meetup
   JSON Processing in the Database
   ************************************************
   Author:  Ryan B. Harvey
   Created: January 4, 2015
   ************************************************
   This script demonstrates pulling data from the
   DigitalGov Jobs API directly into a PostgreSQL 
   9.4 table, then manipulating the data into 
   tabular form suitable for future analysis.

   API Docs: http://search.digitalgov.gov/developer/jobs.html
***************************************************/

/* Setup code */
create schema if not exists jobs_api;
set search_path to jobs_api;

/* Cleanup from previous runs */
/*
drop view if exists jobs_api_analysis;
drop table if exists jobs_api_locations_xref;
drop table if exists jobs_api_locations;
drop table if exists jobs_api;
*/

/* We'll start by creating a temp table to dump API data into.
   Note the default values set for the non-JSON fields.
   These allow you to automatically timestamp and userstamp the
   data as it comes into the table.
 */
drop table if exists jobs_api_data_raw;
create temporary table jobs_api_data_raw (
  response jsonb,
  captured_at timestamp with time zone 
              not null 
              default current_timestamp,
  captured_by character varying 
              not null 
              default current_user
);

/* Now, we'll pull data directly (via cURL) from the Jobs API into the response column.
   Here, my query is looking for full-time positions near San Francisco, CA, and pulling a page
   of 100 positions at a time.  I'll grab 500 positions total for this demo.
 */
copy jobs_api_data_raw (response)
from
  program 'curl "http://api.usa.gov/jobs/search.json?lat_lon=37.783333,-122.416667&query=full-time+positions&size=100&from=0"';

copy jobs_api_data_raw (response)
from
  program 'curl "http://api.usa.gov/jobs/search.json?lat_lon=37.783333,-122.416667&query=full-time+positions&size=100&from=100"';

copy jobs_api_data_raw (response)
from
  program 'curl "http://api.usa.gov/jobs/search.json?lat_lon=37.783333,-122.416667&query=full-time+positions&size=100&from=200"';

copy jobs_api_data_raw (response)
from
  program 'curl "http://api.usa.gov/jobs/search.json?lat_lon=37.783333,-122.416667&query=full-time+positions&size=100&from=300"';

copy jobs_api_data_raw (response)
from
  program 'curl "http://api.usa.gov/jobs/search.json?lat_lon=37.783333,-122.416667&query=full-time+positions&size=100&from=400"';

/* Let's see what we've got */
select * from jobs_api_data_raw;

/* Looks like each pull is a big JSON list.  Let's clean this up a bit and get job objects... */
drop table if exists jobs_api_data;
create temporary table jobs_api_data (
  job jsonb,
  captured_at timestamp with time zone,
  captured_by character varying 
);

/* Recall: jsonb_array_elements returns a set of jsonb objects, one row per list element */
insert into jobs_api_data (job, captured_at, captured_by)
select jsonb_array_elements(response) as job, captured_at, captured_by
from jobs_api_data_raw;

select * from jobs_api_data;

/* Cleanup: drop the raw API responses. */
drop table if exists jobs_api_data_raw;

/* Now, let's look at this in entity-attribute-value view: job ID, object key and associated value */
select job->>'id' as job_id, 
       (jsonb_each(job)).key, 
       (jsonb_each(job)).value 
from jobs_api_data;

/* Let's see what keys we have in these objects... */
select distinct jsonb_object_keys(job)
from jobs_api_data;

/* We'll create a table to capture each of these elements.  Best guess on data types for now... */
create table if not exists jobs_api (
  id text, 
  url text, 
  position_title text,
  minimum integer, 
  maximum integer, 
  rate_interval_code text,
  start_date date,
  end_date date,
  locations jsonb, /* looks like a list, so keep as JSON value */
  captured_at timestamp with time zone,
  captured_by character varying
);

/* Let's try to pull this data into the table */
/* 
-- This one errors out: uncomment to see the error.

insert into jobs_api (id, url, maximum, minimum, rate_interval_code, end_date,
                     locations, start_date, position_title, captured_at, captured_by)
with records as (
  select 
    jsonb_populate_record(null::jobs_api, job) as job_record, 
    captured_at, captured_by
  from jobs_api_data
)
select 
  (job_record).id, 
  (job_record).url,
  (job_record).maximum,
  (job_record).minimum,
  (job_record).rate_interval_code,
  (job_record).end_date,
  (job_record).locations,
  (job_record).start_date,
  (job_record).position_title,
  captured_at,
  captured_by
from records;
 */

/* ! ACK! Error on insert because there's a number with a decimal in there somewhere. 
   Let's fix that!  First, what records are causing the problem? */
select job->'id', job->'minimum', job->'maximum'
from jobs_api_data
where job->>'minimum' like '%.%' or job->>'maximum' like '%.%';

/* Looks like we have some numbers ending in '.0' -- no data loss to just truncate...
   To do that, we'll create a copy of the jobs_api table as a temp, then change the 
   INTEGER type columns to TEXT.
 */
drop table if exists jobs_api_temp;
create temporary table jobs_api_temp ( like jobs_api );

alter table jobs_api_temp 
alter column minimum set data type text,
alter column maximum set data type text;

/* Then, we'll dump the data into that, casting minimum and maximum as TEXT. */
insert into jobs_api_temp (id, url, maximum, minimum, rate_interval_code, end_date, 
                          locations, start_date, position_title, captured_at, captured_by)
with records as (
  select 
    jsonb_populate_record(null::jobs_api_temp, job) as job_record,
    captured_at, captured_by
  from jobs_api_data
)
select 
  (job_record).id, 
  (job_record).url,
  (job_record).maximum::text,
  (job_record).minimum::text,
  (job_record).rate_interval_code,
  (job_record).end_date,
  (job_record).locations,
  (job_record).start_date,
  (job_record).position_title,
  captured_at,
  captured_by
from records;

select * from jobs_api_temp;

/* Cleanup: drop the jobs_api_data temp table. */
drop table if exists jobs_api_data;

/* Now, to deal with those pesky decimals. */
select * from jobs_api_temp
where maximum like '%.%' or minimum like '%.%';

/* We can truncate these several ways... I'm using a regular expression replace. */
update jobs_api_temp
set maximum = regexp_replace(maximum, '\.\d+', '')::integer,
    minimum = regexp_replace(minimum, '\.\d+', '')::integer;

select * from jobs_api_temp;

/* Now, let's try our insert into the real table from this one, casting minimum 
   and maximum to INTEGER as we go.
 */
insert into jobs_api (id, url, maximum, minimum, rate_interval_code, end_date, 
                     locations, start_date, position_title, captured_at, captured_by)
select 
  id, url, maximum::integer, minimum::integer, rate_interval_code, 
  end_date, locations, start_date, position_title, captured_at, captured_by
from jobs_api_temp;

/* Let's take a look at what we've got so far... */
select * from jobs_api;

/* Cleanup: drop the jobs_api_temp temp table */
drop table if exists jobs_api_temp;

/* How many total locations do we have? */
select count(*) from (
  select distinct jsonb_array_elements(locations) as city 
  from jobs_api
) t;

/* Let's put these locations into their own table. */
create table if not exists jobs_api_locations (
  id serial primary key,
  city text,
  state char(2)
);

/* First, we need a place to dump all the text versions. */
drop table if exists temp_locations;
create temporary table temp_locations (
  loc text
);

insert into temp_locations (loc)
select distinct jsonb_array_elements_text(locations) as loc 
from jobs_api;

select loc from temp_locations;

/* Now, let's split them into city and state and put them into the table */
insert into jobs_api_locations (city, state)
select regexp_replace(loc, ', [A-Z]{2}$', '') as city,
       case when loc ~ ', [A-Z]{2}$'
            then right(loc, 2) 
            else null end as state
from temp_locations;

/* Cleanup: drop the temp_locations temp table */
drop table if exists temp_locations;

/* Finally, let's cross-reference the locations to the jobs */
create table if not exists jobs_api_locations_xref (
  id serial primary key,
  jobs_api_id text,
  location_id bigint
);

/* As before, we'll start with a temp table to hold the text version */
drop table if exists job_loc_map_temp;
create temporary table job_loc_map_temp (
  job_id text,
  loc text
);

insert into job_loc_map_temp (job_id, loc)
select id as job_id, jsonb_array_elements_text(locations) as loc
from jobs_api;

/* Then, we'll look at how to join these together... */
select *
from job_loc_map_temp t 
  left join jobs_api_locations l
    on l.city || ', ' || l.state = t.loc;

/* Now, we'll insert that data into our new cross-reference table */
insert into jobs_api_locations_xref (jobs_api_id, location_id)
select t.job_id as jobs_api_id, l.id as location_id
from job_loc_map_temp t 
  left join jobs_api_locations l
    on l.city || ', ' || l.state = t.loc;

select * from jobs_api_locations_xref;

/* Cleanup: drop the job_loc_map_temp temp table */
drop table if exists job_loc_map_temp;

/* Finally, we have something that looks like an analysis table.
   Let's turn that into a view.
 */
create or replace view jobs_api_analysis as (
  select 
    j.id, j.position_title, j.minimum, j.maximum, 
    j.start_date, j.end_date, l.city, l.state, j.url
  from jobs_api j left join jobs_api_locations_xref x on j.id = x.jobs_api_id
    left join jobs_api_locations l on x.location_id = l.id
);

/* Now, we can use this view for analysis: either directly from the
   database via SQL, exported to CSV, or in whatever other way we like.
 */
select * from jobs_api_analysis
order by end_date desc, start_date desc, id;
