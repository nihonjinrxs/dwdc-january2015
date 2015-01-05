# JSON Processing in the Database
##### Getting, processing, and reshaping JSON data using PostgreSQL 9.4

This repository contains materials for [my talk at the Data Wranglers DC meetup on January 7, 2015](http://www.meetup.com/Data-Wranglers-DC/events/219112410/).

I've also given prior talks on SQL to the same group:
* [August 6, 2014](http://www.meetup.com/Data-Wranglers-DC/events/177269432/), materials at [nihonjinrxs/dwdc-august2014](http://www.github.com/nihonjinrxs/dwdc-august2014)
* [June 4, 2014](http://www.meetup.com/Data-Wranglers-DC/events/171768162/), materials at [nihonjinrxs/dwdc-june2014](http://www.github.com/nihonjinrxs/dwdc-june2014)

### The Talk
The talk consists of two major pieces:
- Introductions to the following (via slide deck and presentation):
  - The JavaScript Object Notation (JSON) Data Interchange Format (see [IETF RFC 7159](http://rfc7159.net/rfc7159))
  - Briefly: relational data, SQL, and [PostgreSQL](http://www.postgresql.org)
  - PostgreSQL's implementation in versions 9.2 through 9.4 of [JSON types](http://www.postgresql.org/docs/9.4/interactive/datatype-json.html) and [JSON operators and functions](http://www.postgresql.org/docs/9.4/interactive/functions-json.html)
- A few examples (via PostgreSQL 9.4 flavored SQL code and demonstration):
  - Some demonstration code that shows how to use the JSON operators and JSON processing functions in PostgreSQL 9.4.
  - An example that pulls weather data from the [OpenWeatherMap API](http://openweathermap.org/api) and processes it into tabular data.
  - An example that pulls job opening data from the [DigitalGov Jobs API](http://search.digitalgov.gov/developer/jobs.html) and processes it into tabular data.

Note that the focus of this talk is on data preprocessing tasks (also called wrangling or munging), and not on analysis tasks.  The goal of this work is to demonstrate some ways to use PostgreSQL's capabilities in version 9.4 (released December 18, 2014) to process JSON formatted data into tabular datasets amenable to future analysis.

### Contents
Folders are as follows:
- A slide deck (`./slides`) in Apple Keynote, [PDF](http://nihonjinrxs.github.io/dwdc-january2015/DWDC-January2015-RyanHarvey.pdf) and [HTML](http://nihonjinrxs.github.io/dwdc-january2015) formats
- A set of PostgreSQL 9.4 flavor SQL scripts (`./code`) that create the local PostgreSQL database objects demonstrating working with the `json` and `jsonb` types, including:
  - Some demonstration code that shows how to use the JSON operators and JSON processing functions in PostgreSQL 9.4.
  - An example that pulls weather data from the [OpenWeatherMap API](http://openweathermap.org/api) and processes it into tabular data.
  - An example that pulls job opening data from the [DigitalGov Jobs API](http://search.digitalgov.gov/developer/jobs.html) and processes it into tabular data.

### Where do I start?
I recommend that anyone wishing to understand what I've done and presented here should tackle these pieces in order, starting with the slide deck.

### Disclaimer
This work and the opinions expressed here are my own, and do not purport to represent the views of my current or former employers.
