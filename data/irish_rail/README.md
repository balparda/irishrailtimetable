# IRISH RAIL DATASET

Daniel Balparda de Carvalho (balparda@gmail.com)

# Data Source and Definition

From: https://data.gov.ie/dataset/gtfs-irish-rail

Format is GTFS: https://en.wikipedia.org/wiki/General_Transit_Feed_Specification

See: https://developers.google.com/transit/gtfs/reference/

Date: 2018-09-10, just after the schedule changes.

More datasets: https://www.transportforireland.ie/transitData/PT_Data.html

License: https://data.gov.ie/pages/opendatalicence

# Observations on data

* Stop hours 24-hour clock goes to "24", so beware on parsing.

* Irish Rail uses `calendar_dates.txt` in a strange way, to remove all dates where a service
  should not be included. So they only have mostly `2`-type entries (exclusions) and few `1`-type
  entries (inclusions).

-Daniel
