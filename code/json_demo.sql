/**************************************************
   Data Wranglers DC : January 2015 Meetup
   JSON Processing in the Database
   ************************************************
   Author:  Ryan B. Harvey
   Created: January 4, 2015
   ************************************************
   This script demonstrates working with JSON data
   in PostgreSQL version 9.4, and is meant to be 
   used as live demonstration alongside the slide
   deck. It allows you to explore the syntax and
   concepts in your own instance of PostgreSQL 9.4
   so that you can better understand how it works.

   Sample data was generated from http://www.mockaroo.com
***************************************************/

/* Setup code */
create schema if not exists json_demo;
set search_path to json_demo;

/* Start over */
drop table if exists json_demo_data_raw;

/* Create a table and put some dummy data in it. */
create table if not exists json_demo_data_raw (
  id serial, 
  text_json json, 
  binary_json jsonb
);

copy json_demo_data_raw (text_json)
from '/Users/ryan/data/dwdc-january2015/code/MOCK_DATA.json';

update json_demo_data_raw
set binary_json = text_json::jsonb;

/* Look at what we have... */
select text_json, binary_json
from json_demo_data_raw;

/***************************
 * JSON Operators
 ***************************/

/* We can get a JSON value out of a list by using the -> operator. */
select text_json->2, binary_json->2
from json_demo_data_raw;

/* This operator is chainable, and also works for objects. */
select text_json->2->'id', binary_json->2->'email'
from json_demo_data_raw;

/* Using a double arrowhead gives us a text value return type. */
select text_json->2->>'id', binary_json->2->>'email'
from json_demo_data_raw;

/* Instead of chaining, we can use the #> and #>> operators, which walk the object. */
select text_json#>'{2,id}', binary_json#>'{2,email}'
from json_demo_data_raw;

select text_json#>>'{2,id}', binary_json#>>'{2,email}'
from json_demo_data_raw;

/***************************
 * JSON Parsing Functions
 ***************************/

/* If our JSON is a list (which it is), we can get the number of items in it. */
select json_array_length(text_json), jsonb_array_length(binary_json)
from json_demo_data_raw;

/* We can also get all the elements as rows. */
select json_array_elements(text_json), jsonb_array_elements(binary_json)
from json_demo_data_raw;

/* Let's put that into a table to use with the object parsing functions. */
create table json_demo_data (
  id serial, 
  text_json json, 
  binary_json jsonb
);

insert into json_demo_data (text_json, binary_json)
select 
  json_array_elements(text_json) as text_json, 
  jsonb_array_elements(binary_json) as binary_json
from json_demo_data_raw;

/* With a JSON object (which we now have), we can get all the keys as rows. */
select json_object_keys(text_json), jsonb_object_keys(binary_json)
from json_demo_data;

/* If we want to know all possible unique field keys across all objects, 
   we can use SELECT DISTINCT. 
 */
select distinct json_object_keys(text_json), jsonb_object_keys(binary_json)
from json_demo_data;

/* If we want, we can pivot our data so that it is in Entity-Attribute-Value format. */
select 
  text_json->>'id' as entity, 
  (json_each(text_json)).key as attr,
  (json_each(text_json)).value as val
from json_demo_data;

select 
  binary_json->>'id' as entity, 
  (jsonb_each(binary_json)).key as attr,
  (jsonb_each(binary_json)).value as val
from json_demo_data;

/* Using the above view of our data, we can also create a table structure with 
 * columns named for each field, and populate it directly. 
 */
create table if not exists json_data_tabular (
  id text,
  last_name text,
  first_name text,
  email text,
  phone text,
  gender character(1),
  company text,
  "position" json
);

/* Now, we'll use the table as a record type. */
select json_populate_record(null::json_data_tabular, text_json)
from json_demo_data;

/* Note that we can get to the columns of this record type via dot [(record).column] notation. */
with record_data (my_record) as (
  select json_populate_record(null::json_data_tabular, text_json) as my_record
  from json_demo_data
)
select (my_record).id, (my_record).phone
from record_data;

/* Now, we can insert into our table via the json_populate_record(...) select statement. */
insert into json_data_tabular (id, last_name, first_name, email, phone, 
                               gender, company, "position")
select (t.a_row).id, (t.a_row).last_name, (t.a_row).first_name, (t.a_row).email, 
       (t.a_row).phone, (t.a_row).gender, (t.a_row).company, (t.a_row).position
from (
  select json_populate_record(null::json_data_tabular, text_json) as a_row
  from json_demo_data
) t;

select * from json_data_tabular;

/* We can do the same thing using the jsonb type. */
create table if not exists jsonb_data_tabular (
  id text,
  last_name text,
  first_name text,
  email text,
  phone text,
  gender character(1),
  company text,
  "position" jsonb
);

insert into jsonb_data_tabular (id, last_name, first_name, email, phone, 
                               gender, company, "position")
select (t.a_row).id, (t.a_row).last_name, (t.a_row).first_name, (t.a_row).email, 
       (t.a_row).phone, (t.a_row).gender, (t.a_row).company, (t.a_row).position
from (
  select jsonb_populate_record(null::jsonb_data_tabular, binary_json) as a_row
  from json_demo_data
) t;

select * from jsonb_data_tabular;

/* With a binary JSON (jsonb) column, we can test for existence of a key using the ? operator. */
select id, "position" ? 'bonus' as got_bonus from jsonb_data_tabular;

/* This can be used as a filter condition in a WHERE clause. */
select id, last_name, first_name
from jsonb_data_tabular
where "position" ? 'bonus';

/* We can also test for any one of a list of keys using the ?| operator, and all 
 * of a list of keys using the ?& operator. 
 */
select "position" ?| array['bonus', 'salary'] from jsonb_data_tabular;

select "position" ?& array['bonus', 'salary'] from jsonb_data_tabular; 

/* Finally, we can test object containment with the @> (or <@) operator. */
select binary_json @> '{"gender":"F", "company":"Mybuzz"}'
from json_demo_data;

select '{"gender":"F", "company":"Mybuzz"}' <@ binary_json
from json_demo_data; -- same result as above...

/* To improve query performance on jsonb columns for key existence and containment queries,
 * we can add a GIN "jsonb_ops" index.  If we only need containment, we can use "jsonb_path_ops"
 * instead, which is much smaller on disk.
 */
create index idx_gin_jo_json_demo_data on json_demo_data using gin (binary_json);

create index idx_gin_jpo_json_demo_data on json_demo_data using gin (binary_json jsonb_path_ops)
