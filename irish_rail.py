#!/usr/bin/python3 -O
#
# Copyright 2018 Daniel Balparda (balparda@gmail.com)
#
"""Irish Rail data converter."""

import csv
import datetime
import functools
import logging
# import pdb

import click
# TODO: http://click.pocoo.org/5/setuptools/#setuptools-integration
import prettytable

__author__ = 'balparda@gmail.com (Daniel Balparda)'
__version__ = (1, 0)


# log format string
_LOG_FORMAT = '%(asctime)-15s: %(module)s/%(funcName)s/%(lineno)d: %(message)s'
_VERBOSITY_LEVELS = {
    0: logging.ERROR,
    1: logging.INFO,
    2: logging.DEBUG,
}

# rail data dir
_DATA_DIR = 'data/irish_rail/'

# some util consts and lambdas
_DIRECTION = lambda bool_direction_id: 'SOUTH' if bool_direction_id else 'NORTH'
_WEEK_TYPE = {
    0: 'Mon-Fri',
    1: 'Saturday',
    2: 'Sunday',
}
_DATE_REPR = '%Y%m%d'
_LOOK_AHEAD_IN_DAYS = 14
_ONE_DAY = datetime.timedelta(days=1)


class Error(Exception):
  """Irish Rail base exception."""


def _LoadRoutes():  # like {route_id: route_long_name}
  with open(_DATA_DIR + 'routes.txt', 'rt', newline='') as csv_file:
    routes_reader = csv.reader(csv_file)
    next(routes_reader)  # we must skip the first line as it has the header!
    return {route_id: route_long_name for route_id, _, _, route_long_name, _ in routes_reader}


def _RouteIDByName(routes, desired_route):
  route_ids = set()
  for route_id, route_long_name in routes.items():
    if route_long_name == desired_route:
      route_ids.add(route_id)
  if not route_ids:
    raise Error('Route %r not found: use "list" command to see available routes' % desired_route)
  return route_ids


def _LoadStops():  # like {stop_id: stop_name}
  with open(_DATA_DIR + 'stops.txt', 'rt', newline='') as csv_file:
    stops_reader = csv.reader(csv_file)
    next(stops_reader)  # we must skip the first line as it has the header!
    return {stop_id: stop_name for stop_id, stop_name, _, _ in stops_reader}


def _StopIDsByName(stops, desired_stop_name):
  stop_ids = set()
  for stop_id, stop_name in stops.items():
    if stop_name == desired_stop_name:
      stop_ids.add(stop_id)
  if not stop_ids:
    raise Error('Stop %r not found: use "list" command to see available stops' % desired_stop_name)
  return stop_ids


def _LoadTripsForRoute(desired_route_ids, service_exclusions):
  # like {trip_id: (service_id, bool_direction_id)}
  with open(_DATA_DIR + 'trips.txt', 'rt', newline='') as csv_file:
    trips_reader = csv.reader(csv_file)
    next(trips_reader)  # we must skip the first line as it has the header!
    return {trip_id: (service_id, bool(int(direction_id)))
            for route_id, service_id, trip_id, _, _, direction_id in trips_reader
            if route_id in desired_route_ids and service_id not in service_exclusions}


def _LoadTimetableForTrips(desired_trips):
  # like {trip_id: [(int_stop_sequence, stop_id, arrival_time), (), ...]}
  with open(_DATA_DIR + 'stop_times.txt', 'rt', newline='') as csv_file:
    timetable_reader = csv.reader(csv_file)
    next(timetable_reader)  # we must skip the first line as it has the header!
    # read the timetable
    timetable = {}
    for trip_id, arrival_time, _, stop_id, stop_sequence, _, _, _, _ in timetable_reader:
      if trip_id in desired_trips:
        hour, minute, _ = arrival_time.split(':')
        timetable.setdefault(trip_id, []).append(
            (int(stop_sequence), stop_id, datetime.time(int(hour) % 24, int(minute))))
    # make sure stops are sorted before returning
    for stops in timetable.values():  # pylint: disable=not-an-iterable
      stops.sort()
    return timetable


def _GetTripStarts(trips, timetable):  # like {start_time: {trip_id_1, trip_id_2, ...}}
  trip_starts = {}
  for trip_id in trips:
    start_time = timetable[trip_id][0][2]
    trip_starts.setdefault(start_time, set()).add(trip_id)
  return trip_starts


def _AllDatesInPeriod(initial_date, final_date):
  dt = initial_date
  while dt <= final_date:
    yield dt
    dt += _ONE_DAY


def _LoadServiceDates(timetable_datetime):
  # like {service_id: (bool_working_days, bool_saturday, bool_sunday, bool_irregular)}
  # service_exclusions like {service_id1, service_id2, ...}
  # start by reading and storing all the exclusion dates for all services
  date_inclusions, date_exclusions = {}, {}
  with open(_DATA_DIR + 'calendar_dates.txt', 'rt', newline='') as csv_file:
    exclusions_reader = csv.reader(csv_file)
    next(exclusions_reader)  # we must skip the first line as it has the header!
    for service_id, date, exception_type in exclusions_reader:
      date, exception_type = datetime.datetime.strptime(date, _DATE_REPR), int(exception_type)
      if exception_type == 1:
        date_inclusions.setdefault(service_id, set()).add(date)
      elif exception_type == 2:
        date_exclusions.setdefault(service_id, set()).add(date)
      else:
        raise Error('Unexpected exception_type in calendar_dates.txt!')
  # determine next _LOOK_AHEAD_IN_DAYS of schedule days to use in filtering services
  look_ahead_days = set(_AllDatesInPeriod(
      timetable_datetime, timetable_datetime + datetime.timedelta(days=_LOOK_AHEAD_IN_DAYS)))
  # now read calendar to get all services and determine exclusions
  with open(_DATA_DIR + 'calendar.txt', 'rt', newline='') as csv_file:
    calendar_reader = csv.reader(csv_file)
    next(calendar_reader)  # we must skip the first line as it has the header!
    # read the weekday spread per service
    service_dates, service_exclusions = {}, set()
    for (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
         start_date, end_date) in calendar_reader:
      # check if the schedule is current
      start_date = datetime.datetime.strptime(start_date, _DATE_REPR)
      end_date = datetime.datetime.strptime(end_date, _DATE_REPR)
      if not start_date <= timetable_datetime <= end_date:
        raise Error(
            'Current date (%s) is outside dates in calendar.txt (%s to %s)! '
            'You need to refresh Irish Rail data!' %
            (timetable_datetime.strftime(_DATE_REPR),
             start_date.strftime(_DATE_REPR), end_date.strftime(_DATE_REPR)))
      # generate all dates in period
      period_dates = set(_AllDatesInPeriod(start_date, end_date)).union(
          date_inclusions.get(service_id, set())) - date_exclusions.get(service_id, set())
      if any(d not in period_dates for d in look_ahead_days):
        logging.warning(
            'Removing service_id %r because not all days in next %d are in this period',
            service_id, _LOOK_AHEAD_IN_DAYS)
        service_exclusions.add(service_id)
      # convert all to bool
      monday, tuesday, wednesday, thursday, friday, saturday, sunday = (
          bool(int(monday)), bool(int(tuesday)), bool(int(wednesday)), bool(int(thursday)),
          bool(int(friday)), bool(int(saturday)), bool(int(sunday)))
      # figure out working day (Mon-Fri) schedule and irregular schedules
      bool_working_days = monday and tuesday and wednesday and thursday and friday
      bool_irregular = (
          (not bool_working_days and not saturday and not sunday) or  # no useful cathegories found
          (not bool_working_days and                                  # not all weekdays
           any((monday, tuesday, wednesday, thursday, friday))))
      service_dates[service_id] = (bool_working_days, saturday, sunday, bool_irregular)
    return (service_dates, service_exclusions)


def _CmpStopsByFirstAvailableTime(a, b):
  # first we compare by 'week' type
  if a['week'] != b['week']:
    return 1 if a['week'] > b['week'] else -1
  # then we compare by stop times
  for i, (_, time_a) in enumerate(a['stops']):
    time_b = b['stops'][i][1]
    # skip the None times and the ones that are equal
    if time_a and time_b and time_a != time_b:
      # this is the one to compare by
      return 1 if time_a > time_b else -1
  # they were all missing or equal (happens: a and b can be the same)
  return 0


def _WriteCSVs(desired_rout_names, timetable_date_to_use, output_tables):
  route_path = ('_'.join(desired_rout_names)).replace(' ', '_').replace('/', '_')
  for bool_direction_id in sorted(output_tables):
    trips_table = output_tables[bool_direction_id]
    output_path = '%s_%s_%s.csv' % (
        route_path, _DIRECTION(bool_direction_id), timetable_date_to_use.strftime(_DATE_REPR))
    logging.info('Saving %r', output_path)
    with open(output_path, 'wt', newline='') as csv_file:
      route_writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
      for row in trips_table:
        route_writer.writerow(row)


def _PrintTables(desired_rout_names, output_tables):
  for bool_direction_id in sorted(output_tables):
    trips_table = output_tables[bool_direction_id]
    pt_obj = prettytable.PrettyTable(trips_table[0])
    for row in trips_table[1:]:
      pt_obj.add_row(row)
    click.echo()
    click.echo('%s %s table' % ('/'.join(desired_rout_names), _DIRECTION(bool_direction_id)))
    click.echo()
    click.echo(pt_obj)
    click.echo()


def _PrintRoutes(routes):
  click.echo()
  r_obj = prettytable.PrettyTable(['Official Route Name'])
  for name in sorted({n for n in routes.values()}):
    r_obj.add_row([name])
  click.echo(r_obj)


def _PrintStops(stops_names):
  click.echo()
  s_obj = prettytable.PrettyTable(['Official Station Name'])
  for name in sorted({n for n in stops_names.values()}):
    s_obj.add_row([name])
  click.echo(s_obj)


@click.command()
# see `click` module usage in:
#   http://click.pocoo.org/5/quickstart/
#   http://click.pocoo.org/5/options/
#   http://click.pocoo.org/5/documentation/#help-texts
@click.argument('operation', type=click.Choice(['list', 'print']))
@click.option(
    '--route', '-r', 'routes_tuple', type=click.STRING, multiple=True,
    help='Case-sensitive Irish Rail route/service name (ex: "DART"); '
    'can be given more than once for multiple routes/services; at least one required.')
@click.option(
    '--stop', '-s', 'stops_tuple', type=click.STRING, multiple=True,
    help='Case-sensitive Irish Rail stop name to include in output (ex: "Grand Canal Dock"); '
    'can be given more than once for multiple stops, and at least 2 are required.')
@click.option(
    '--alias', '-a', 'aliases_tuple', type=(click.STRING, click.STRING), multiple=True,
    help='Station alias as 2 strings, the first is the case-sensitive Irish Rail name and the '
    'second is the alias (ex: -a "Bray Daly" "Bray"); can be given more than once.')
@click.option(
    '--fake', '-f', 'fakes_tuple', type=(click.STRING, click.STRING, click.INT), multiple=True,
    help='Fake (inserted) station to display in output as 2 strings and an int delta in number '
    'of minutes before or after; the first string is the alias for the fake station, the second '
    'string is the Irish Rail case-sensitive station to count the delta from, and the integer is '
    'the delta, in whole minutes (ex: -f "Home" "Bray Daly" -10, meaning "Home" is 10 min south '
    'of Bray); this delta is counted towards the bool_direction_id==False direction of the data '
    'table (on the DART service this means the NORTH directon); can be given more than once.')
@click.option(
    '--print-out/--no-print-out', 'print_out', default=True,
    help='Print to screen? Default is yes (--print-out).')
@click.option(
    '--csv-out/--no-csv-out', 'csv_out', default=False,
    help='Save CSV file? Default is no (--no-csv-out).')
@click.option(
    '--idcol/--no-idcol', 'idcol_out', default=False,
    help='Add Irish Rail trip ID column to output? Default is no (--no-idcol).')
@click.option(
    '--date', '-d', 'date_to_use', type=click.STRING, default='',
    help='If given, the date that will be used for producing timetables; '
    'Format has to be YYYYMMDD; If not given, current date will be used.')
@click.option(
    '--irregular/--no-irregular', 'allow_irregulars', default=False,
    help='Dangerous; Allow for irregular schedules; By default (--no-irregular) will skip all '
    'irregular schedules even if they serve some days that could be listed; an example would be a '
    'trip schedule that serves Mon & Sat but not other weekdays: it is irregular but could be '
    'listed on Sat\'s schedule even if not listed as a working day.')
@click.option(
    '--max-trips', 'max_trips', type=click.IntRange(0, 50000, clamp=True), default=0,
    help='Dangerous; If given, will limit the number of trips (rows) in the output, for debugging.')
@click.option(
    '--verbose', '-v', 'verbosity_level', count=True,
    help='Verbose level; default is errors only; -v includes info/warning; -vv includes debug.')
def tables(
    operation, routes_tuple, stops_tuple, aliases_tuple, fakes_tuple,
    print_out, csv_out, idcol_out, date_to_use, allow_irregulars, max_trips, verbosity_level):
  """Load Irish Rail route data and output custom timetables. OPERATION is either "list" to
  show Irish Rail official route and station names or "print" to produce a custom timetable
  for certain stops. Typical examples:

  \b
  ./irish_rail.py list
  ./irish_rail.py print --csv-out --route DART \\
      --stop "Howth Junction and Donaghmede" \\
      --stop "Tara St" \\
      --stop "Grand Canal Dock" \\
      --alias "Bray Daly" "Bray" \\
      --alias "Howth Junction and Donaghmede" "Howth Junction" \\
      --alias "Grand Canal Dock" "Grand Canal" \\
      --fake "Home" "Howth Junction and Donaghmede" 24 \\
      --fake "Work Desk" "Grand Canal Dock" -9
  """
  # set logging level
  logging.basicConfig(
      level=_VERBOSITY_LEVELS[verbosity_level if verbosity_level <= 2 else 2], format=_LOG_FORMAT)
  logging.info('START: IRISH RAIL CUSTOM TIMETABLE')
  # load oficial routes and stops first, as we might need to print those
  routes, stops_names = _LoadRoutes(), _LoadStops()
  if operation == 'list':
    logging.info('OPERATION: list routes & stops')
    _PrintRoutes(routes)
    _PrintStops(stops_names)
    return
  logging.info('OPERATION: print custom timetable')
  # process basic flags
  if not print_out and not csv_out:
    click.echo('With --no-print-out and --no-csv-out there is nothing to do!')
    return
  desired_rout_names = {r.strip() for r in routes_tuple if r.strip()}
  desired_route_ids = set()
  for route_name in desired_rout_names:
    desired_route_ids.update(_RouteIDByName(routes, route_name))
  stops_set = {s.strip() for s in stops_tuple if s.strip()}
  if not desired_rout_names or len(stops_set) < 2:
    click.echo('With less than one --route and two --stop there is nothing to do!')
    return
  aliases_dict = {s.strip(): a.strip() for s, a in aliases_tuple if s.strip()}
  fakes_dict = {a.strip(): (s.strip(), d) for a, s, d in fakes_tuple if s.strip()}
  # compute date and some date utils
  date_to_use = date_to_use.strip()
  timetable_date_to_use = (datetime.datetime.strptime(date_to_use, _DATE_REPR).date()
                           if date_to_use else datetime.date.today())
  timetable_date_repr = timetable_date_to_use.strftime(_DATE_REPR)
  timetable_datetime = datetime.datetime.combine(timetable_date_to_use, datetime.time(hour=0))
  add_minutes_to_time = lambda t, m: (datetime.datetime.combine(timetable_date_to_use, t) +
                                      datetime.timedelta(minutes=m)).time()
  # log the options
  logging.info('Routes: %s', ', '.join(repr(n) for n in sorted(desired_rout_names)))
  logging.info('Stations: %s', ', '.join(repr(n) for n in sorted(stops_set)))
  logging.info(
      'Aliases: %s', ', '.join('%r=%r' % (n, aliases_dict[n]) for n in sorted(aliases_dict)))
  logging.info(
      'Fake Stations: %s', ', '.join(
          '%r=%r/%dmin' % (n, fakes_dict[n][0], fakes_dict[n][1]) for n in sorted(fakes_dict)))
  logging.info('Date: %s', timetable_date_repr)
  # get stops and find the interesting ones; note one stop name can translate to multiple IDs
  station_aliases = {
      stop_id: stop_alias
      for stop_name, stop_alias in aliases_dict.items()
      for stop_id in _StopIDsByName(stops_names, stop_name) if stop_alias}
  translate_stop_name = lambda stop_id: station_aliases.get(stop_id, None) or stops_names[stop_id]
  interesting_stops = {stop_name: _StopIDsByName(stops_names, stop_name) for stop_name in stops_set}
  desired_stops_count = len(stops_set)
  interesting_stops_ids = {stop_id for stops in interesting_stops.values() for stop_id in stops}
  # add "fake stops" and then monkey-patch the fake stops into the structures above
  fake_stops = {stop_id: (fake_name, rel_min)
                for fake_name, (rel_stop_name, rel_min) in fakes_dict.items()  # pylint: disable=not-an-iterable
                for stop_id in _StopIDsByName(stops_names, rel_stop_name)}
  for fake_name, rel_min in fake_stops.values():
    interesting_stops[fake_name] = {fake_name}  # NOTE: for fake stops the ID and name are the same!
    stops_names[fake_name] = fake_name
    interesting_stops_ids.add(fake_name)
    if not rel_min:
      raise Error(
          'Fake stations cannot have zero delay from an actual station (on %r)' % fake_name)
  desired_stops_count += len(fakes_dict)
  # get the calendar
  service_dates, service_exclusions = _LoadServiceDates(timetable_datetime)
  # load trips and timetables, filtered by the desired route
  # TODO: include feature to allow multiple desired routes so user can look at more complete data
  trips = _LoadTripsForRoute(desired_route_ids, service_exclusions)
  timetable = _LoadTimetableForTrips(set(trips))
  # get all the trips (but only for the interesting stops)
  trip_starts = _GetTripStarts(trips, timetable)
  # now we calculate the data to be output, which is like:
  #   {bool_direction_id: [
  #       {'id': trip_id, 'week': week_type,
  #        'start': (stop_id, arrival_time), 'end': (stop_id, arrival_time),
  #        'stops': [(stop_id, arrival_time), ...more stops...]}, ...more trips...]}
  output_dict = {False: [], True: []}
  for bool_direction_id in sorted(output_dict):
    trips_table = output_dict[bool_direction_id]
    for week_index in sorted(_WEEK_TYPE):
      for trip_start in sorted(trip_starts):
        for trip_id in sorted(trip_starts[trip_start]):  # we sort by trip start
          # get service dates and filter by them
          trip = trips[trip_id]
          service_id = trip[0]
          week_schedule = service_dates[service_id]
          if not allow_irregulars and week_schedule[3]:
            logging.debug('Skipping trip %s because it has an irregular schedule', trip_id)
            continue
          if trip[1] == bool_direction_id and week_schedule[week_index]:
            # here we have a trip we may want to output, so go figure it out
            trip_timetable = timetable[trip_id]
            from_station, to_station = trip_timetable[0], trip_timetable[-1]
            filtered_stations = tuple(s for s in trip_timetable if s[1] in interesting_stops_ids)
            if len(filtered_stations) >= 2:
              # this trip has at least 2 stops from our list, so it is to be output
              trip_stops = []
              for _, stop_id, arrival_time in filtered_stations:
                # we have a stop to add, but it might have a fake stop associated to it, so here
                # is where we will add it as if it was part of the line's schedule; we have to be
                # careful to add it either before or after the master station
                fake_name, rel_min = fake_stops.get(stop_id, (None, None))
                if fake_name is not None and (bool_direction_id == (rel_min > 0)):
                  # fake stop before master
                  trip_stops.append((fake_name, add_minutes_to_time(arrival_time, -abs(rel_min))))
                trip_stops.append((stop_id, arrival_time))  # master stop
                if fake_name is not None and (bool_direction_id != (rel_min > 0)):
                  # fake stop after master
                  trip_stops.append((fake_name, add_minutes_to_time(arrival_time, abs(rel_min))))
              trips_table.append({
                  'id': trip_id, 'week': week_index,
                  'start': from_station[1:], 'end': to_station[1:], 'stops': trip_stops})
  # having the data to output we generate the output
  # first we have to find out the order of the stations for each direction and make sure this
  # is consistent across all data, by checking them and adding None values in the missing ones
  stop_ordering = {}  # like {bool_direction_id: (stop1, stop2, ...)}
  for bool_direction_id in sorted(output_dict):
    trips_table = output_dict[bool_direction_id]
    # find first trip who's stop list has all the desired stops to use as a template
    for trip in trips_table:
      if len(trip['stops']) == desired_stops_count:
        # this is it!
        stop_ordering[bool_direction_id] = tuple(s[0] for s in trip['stops'])
        break
    else:
      raise Error('No trip was found that had all desired stops!')
    # go through all trips to be output and add (stop_id, None) value paddings to missing stations
    # raise if we find a discrepancy in the order
    for trip_id, trip_stops in ((trip['id'], trip['stops']) for trip in trips_table):
      original_i = 0
      for desired_i, desired_stop in enumerate(stop_ordering[bool_direction_id]):
        # compare to what is the expected
        if original_i >= len(trip_stops) or trip_stops[original_i][0] != desired_stop:
          # we found a missing station
          trip_stops.insert(desired_i, (desired_stop, None))
        original_i += 1
      # if we had an inverted order, the side effect of the above is to end up with a trip_stops
      # that is larger than desired_stops_count
      if len(trip_stops) > desired_stops_count:
        raise Error('Found trip with iverted directionality: %s' % trip_id)
    # now we can re-sort so we have the first available stop
    trips_table.sort(key=functools.cmp_to_key(_CmpStopsByFirstAvailableTime))
  # now we can build the actual output tables
  output_tables = {False: [], True: []}
  for bool_direction_id in sorted(output_tables):
    trips_table = output_tables[bool_direction_id]
    trips_dict_table = output_dict[bool_direction_id]
    # put the header in as the first line
    header = ['Trip ID', 'Days', 'Origin'] if idcol_out else ['Days', 'Origin']
    for stop_id in stop_ordering[bool_direction_id]:
      header.append(translate_stop_name(stop_id))
    header.append('Destination')
    trips_table.append(tuple(header))
    # add trips data
    for output_trips_count, trip in enumerate(trips_dict_table):
      # check for row cap
      if max_trips and output_trips_count >= max_trips:
        logging.warning('Trips count was capped at %d, but there were more.', max_trips)
        break
      # add type and start
      row = [trip['id']] if idcol_out else []
      row.append(_WEEK_TYPE[trip['week']])
      row.append(translate_stop_name(trip['start'][0]))
      # add stops data
      for stop_n, (stop_id, arrival_time) in enumerate(trip['stops']):
        if stop_id != stop_ordering[bool_direction_id][stop_n]:
          raise Error('Inconsistency found in trip %s' % trip['id'])  # should not happen!
        row.append('X' if arrival_time is None else arrival_time.strftime('%H:%M'))
      # add end data
      row.append(translate_stop_name(trip['end'][0]))
      # put row in
      trips_table.append(tuple(row))
  # finally, we save the data to CSV, print it, or both
  if csv_out:
    _WriteCSVs(desired_rout_names, timetable_date_to_use, output_tables)
  if print_out:
    _PrintTables(desired_rout_names, output_tables)
  logging.info('DONE')


# only execute main() if used directly --- not sure how robust this is...
if __name__ == '__main__':
  tables()  # pylint: disable=no-value-for-parameter
