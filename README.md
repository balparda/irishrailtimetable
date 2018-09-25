# Irish Rail Timetable

Irish Rail Timetable: converts GTFS raw data, as published by Ireland's
government, to custom service timetables that can be useful to individual
users of specific train stations. Users can determine which stations are
important, can add aliases, and can add custom stops to represent useful
calculation points to the user.

Focus on DART for now, so not guaranteed to work for other services.
The GTFS should mostly work, but there are some details (like directionality)
that have to be ironed out so this will work for all services. Probably could
be extended to any generic GTFS-published data with medium effort.

Started in 2018/September, by Daniel Balparda de Carvalho.

# License

Copyright (C) 2018 Daniel Balparda de Carvalho (balparda@gmail.com).
This file is part of Irish Rail Timetable.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see http://www.gnu.org/licenses/gpl-3.0.txt.

# Setup

Just the basic, plus `click` and `prettytable` Python 3 packages:

```
$ hg clone https://balparda@bitbucket.org/balparda/irishrailtimetable
$ sudo apt-get install python3-pip pylint3
$ sudo pip3 install -U click prettytable
```

# Data

Data comes from: https://data.gov.ie/dataset/gtfs-irish-rail

Much more info in [`data/irish_rail/README.md`](data/irish_rail/README.md).

# Usage

TODO
