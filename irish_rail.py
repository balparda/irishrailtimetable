#!/usr/bin/python3 -O
#
# Copyright 2018 Daniel Balparda (balparda@gmail.com)
#
"""Irish Rail data converter."""

import csv
import datetime
import logging
# import pdb
import sys

import prettytable

from miscbalpardacode import util

__author__ = 'balparda@gmail.com (Daniel Balparda)'
__version__ = (1, 0)


_DATA_DIR = 'data/irish_rail/'
_DESIRED_ROUT_NAME = 'DART'
_DESIRED_STOPS = ('Howth Junction and Donaghmede', 'Tara St', 'Grand Canal Dock')
# var below: if True will skip *all* irregular schedules even if they serve some days that could
# be listed; example would be a trip schedule that serves Mon & Sat as it is irregular but
# could be listed on Sat's schedule even if not listed as a working day; there are a lot of
# trips like this (that serve Sat or Sun but are irregular on weekdays)
_SKIP_ALL_IRREGULAR_SCHEDULES = True
# var below: controls output media
_WRITE_CSV = True
_PRINT_OUTPUT = True

_DIRECTION = lambda int_direction_id: 'SOUTH' if int_direction_id else 'NORTH'
_WEEK_TYPE = {
    0: 'Mon-Fri',
    1: 'Saturday',
    2: 'Sunday',
}
_ONE_DAY = datetime.timedelta(days=1)
_LOOK_AHEAD_IN_DAYS = 14


def _LoadRoutes():  # like {route_id: route_long_name}
  with open(_DATA_DIR + 'routes.txt', 'rt', newline='') as csv_file:
    routes_reader = csv.reader(csv_file)
    next(routes_reader)  # we must skip the first line as it has the header!
    return {route_id: route_long_name for route_id, _, _, route_long_name, _ in routes_reader}


def _RouteIDByName(routes, desired_route):
  for route_id, route_long_name in routes.items():
    if route_long_name == desired_route:
      return route_id
  raise util.Error('Route %r not found' % desired_route)


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
    raise util.Error('Stop %r not found' % desired_stop_name)
  return stop_ids


def _LoadTripsForRoute(desired_route_id, service_exclusions):
  # like {trip_id: (service_id, int_direction_id)}
  with open(_DATA_DIR + 'trips.txt', 'rt', newline='') as csv_file:
    trips_reader = csv.reader(csv_file)
    next(trips_reader)  # we must skip the first line as it has the header!
    return {trip_id: (service_id, int(direction_id))
            for route_id, service_id, trip_id, _, _, direction_id in trips_reader
            if route_id == desired_route_id and service_id not in service_exclusions}


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
  for trip_id in trips.keys():
    start_time = timetable[trip_id][0][2]
    trip_starts.setdefault(start_time, set()).add(trip_id)
  return trip_starts


def _AllDatesInPeriod(initial_date, final_date):
  dt = initial_date
  while dt <= final_date:
    yield dt
    dt += _ONE_DAY


def _LoadServiceDates():
  # like {service_id: (bool_working_days, bool_saturday, bool_sunday, bool_irregular)}
  # service_exclusions like {service_id1, service_id2, ...}
  # start by reading and storing all the exclusion dates for all services
  date_inclusions, date_exclusions = {}, {}
  with open(_DATA_DIR + 'calendar_dates.txt', 'rt', newline='') as csv_file:
    exclusions_reader = csv.reader(csv_file)
    next(exclusions_reader)  # we must skip the first line as it has the header!
    for service_id, date, exception_type in exclusions_reader:
      date, exception_type = datetime.datetime.strptime(date, '%Y%m%d'), int(exception_type)
      if exception_type == 1:
        date_inclusions.setdefault(service_id, set()).add(date)
      elif exception_type == 2:
        date_exclusions.setdefault(service_id, set()).add(date)
      else:
        raise util.Error('Unexpected exception_type in calendar_dates.txt!')
  # determine next _LOOK_AHEAD_IN_DAYS of schedule days to use in filtering services
  current_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
  look_ahead_days = set(_AllDatesInPeriod(
      current_date, current_date + datetime.timedelta(days=_LOOK_AHEAD_IN_DAYS)))
  # now read calendar to get all services and determine exclusions
  with open(_DATA_DIR + 'calendar.txt', 'rt', newline='') as csv_file:
    calendar_reader = csv.reader(csv_file)
    next(calendar_reader)  # we must skip the first line as it has the header!
    # read the weekday spread per service
    service_dates, service_exclusions = {}, set()
    for (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
         start_date, end_date) in calendar_reader:
      # check if the schedule is current
      start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
      end_date = datetime.datetime.strptime(end_date, '%Y%m%d')
      if not start_date <= current_date <= end_date:
        raise util.Error(
            'Current date (%s) is outside dates in calendar.txt (%s to %s)! '
            'You need to refresh Irish Rail data!' %
            (current_date.strftime('%Y%m%d'),
             start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')))
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


def _WriteCSVs(output_tables):
  route_path = _DESIRED_ROUT_NAME.replace(' ', '_').replace('/', '_')
  for int_direction_id in sorted(output_tables.keys()):
    trips_table = output_tables[int_direction_id]
    output_path = '%s_%s_%s.csv' % (
        route_path, _DIRECTION(int_direction_id),
        datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d'))
    logging.info('Saving %r', output_path)
    with open(output_path, 'wt', newline='') as csv_file:
      route_writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
      for row in trips_table:
        route_writer.writerow(row)


def _PrintTables(output_tables):
  for int_direction_id in sorted(output_tables.keys()):
    trips_table = output_tables[int_direction_id]
    pt_obj = prettytable.PrettyTable(trips_table[0])
    for row in trips_table[1:]:
      pt_obj.add_row(row)
    print()
    print('%s %s table' % (_DESIRED_ROUT_NAME, _DIRECTION(int_direction_id)))
    print()
    print(pt_obj)
    print()


def main(_):
  """Load Irish Rail data and convert timetables to useful data for some stops."""
  logging.info('START: IRISH RAIL TIMETABLE')
  # get routes and find the one we're looking at now
  routes = _LoadRoutes()
  desired_route_id = _RouteIDByName(routes, _DESIRED_ROUT_NAME)
  # get stops and find the interesting ones; note one stop name can translate to multiple IDs
  stops = _LoadStops()
  interesting_stops = {stop_name: _StopIDsByName(stops, stop_name) for stop_name in _DESIRED_STOPS}
  interesting_stops_ids = {
      stop_id for stops_set in interesting_stops.values() for stop_id in stops_set}
  # get the calendar
  service_dates, service_exclusions = _LoadServiceDates()
  # load trips and timetables, filtered by the desired route
  # TODO: include feature to allow multiple desired routes so user can look at more complete data
  trips = _LoadTripsForRoute(desired_route_id, service_exclusions)
  timetable = _LoadTimetableForTrips(set(trips.keys()))
  # get all the trips, sorted by start time, only for the interesting stops
  trip_starts = _GetTripStarts(trips, timetable)
  # now we calculate the data to be output, which is like:
  #   {int_direction_id: [
  #       {'id': trip_id, 'week': week_type,
  #        'start': (stop_id, arrival_time), 'end': (stop_id, arrival_time),
  #        'stops': [(stop_id, arrival_time), ...more stops...]}, ...more trips...]}
  output_dict = {0: [], 1: []}
  for int_direction_id in sorted(output_dict.keys()):
    trips_table = output_dict[int_direction_id]
    for week_index in sorted(_WEEK_TYPE.keys()):
      for trip_start in sorted(trip_starts.keys()):
        for trip_id in sorted(trip_starts[trip_start]):
          # get service dates and filter by them
          trip = trips[trip_id]
          service_id = trip[0]
          week_schedule = service_dates[service_id]
          if _SKIP_ALL_IRREGULAR_SCHEDULES and week_schedule[3]:
            logging.debug('Skipping trip %s because it has an irregular schedule', trip_id)
            continue
          if trip[1] == int_direction_id and week_schedule[week_index]:
            # here we have a trip we may want to output, so go figure it out
            trip_timetable = timetable[trip_id]
            from_station, to_station = trip_timetable[0], trip_timetable[-1]
            filtered_stations = tuple(s for s in trip_timetable if s[1] in interesting_stops_ids)
            if len(filtered_stations) >= 2:
              # this trip has at least 2 stops from our list, so it is to be output
              trip_stops = []
              for _, stop_id, arrival_time in filtered_stations:
                trip_stops.append((stop_id, arrival_time))
              trips_table.append({
                  'id': trip_id, 'week': week_index,
                  'start': from_station[1:], 'end': to_station[1:], 'stops': trip_stops})
  # having the data to output we generate the output
  # first we have to find out the order of the stations for each direction and make sure this
  # is consistent across all data, by checking them and adding None values in the missing ones
  stop_ordering = {}  # like {int_direction_id: (stop1, stop2, ...)}
  for int_direction_id in sorted(output_dict.keys()):
    trips_table = output_dict[int_direction_id]
    # find first trip who's stop list has all the desired stops to use as a template
    for trip in trips_table:
      if len(trip['stops']) == len(_DESIRED_STOPS):
        # this is it!
        stop_ordering[int_direction_id] = tuple(s[0] for s in trip['stops'])
        break
    else:
      raise util.Error('No trip was found that had all desired stops!')
    # go through all trips to be output and add (stop_id, None) value paddings to missing stations
    # raise if we find a discrepancy in the order
    for trip_id, trip_stops in ((trip['id'], trip['stops']) for trip in trips_table):
      original_i = 0
      for desired_i, desired_stop in enumerate(stop_ordering[int_direction_id]):
        # compare to what is the expected
        if original_i >= len(trip_stops) or trip_stops[original_i][0] != desired_stop:
          # we found a missing station
          trip_stops.insert(desired_i, (desired_stop, None))
        original_i += 1
      # if we had an inverted order, the side effect of the above is to end up with a trip_stops
      # that is larger than _DESIRED_STOPS
      if len(trip_stops) > len(_DESIRED_STOPS):
        raise util.Error('Found trip with iverted directionality: %s' % trip_id)
  # now we can build the actual output tables
  output_tables = {0: [], 1: []}
  for int_direction_id in sorted(output_tables.keys()):
    trips_table = output_tables[int_direction_id]
    trips_dict_table = output_dict[int_direction_id]
    # put the header in as the first line
    header = ['Trip ID', 'Days', 'Origin']
    for stop_id in stop_ordering[int_direction_id]:
      header.append(stops[stop_id])
    header.append('Destination')
    trips_table.append(tuple(header))
    # add trips data
    for trip in trips_dict_table:
      row = [trip['id'], _WEEK_TYPE[trip['week']], stops[trip['start'][0]]]
      # add stops data
      for stop_n, (stop_id, arrival_time) in enumerate(trip['stops']):
        if stop_id != stop_ordering[int_direction_id][stop_n]:
          raise util.Error('Inconsistency found in trip %s' % trip['id'])  # should not happen!
        row.append('X' if arrival_time is None else arrival_time.strftime('%H:%M'))
      row.append(stops[trip['end'][0]])
      trips_table.append(tuple(row))
  # finally, we save the data to CSV, print it, or both
  if _WRITE_CSV:
    _WriteCSVs(output_tables)
  if _PRINT_OUTPUT:
    _PrintTables(output_tables)
  logging.info('DONE')


# only execute main() if used directly --- not sure how robust this is...
if __name__ == '__main__':
  sys.exit(main(sys.argv))
