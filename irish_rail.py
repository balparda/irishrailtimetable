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

from miscbalpardacode import util

__author__ = 'balparda@gmail.com (Daniel Balparda)'
__version__ = (1, 0)


_DATA_DIR = 'data/irish_rail/'
_DESIRED_ROUT_NAME = 'DART'
_DESIRED_STOPS = ('Howth Junction and Donaghmede', 'Tara St', 'Grand Canal Dock')


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


def _LoadTripsForRoute(desired_route_id):  # like {trip_id: (service_id, int_direction_id)}
  with open(_DATA_DIR + 'trips.txt', 'rt', newline='') as csv_file:
    trips_reader = csv.reader(csv_file)
    next(trips_reader)  # we must skip the first line as it has the header!
    return {trip_id: (service_id, int(direction_id))
            for route_id, service_id, trip_id, _, _, direction_id in trips_reader
            if route_id == desired_route_id}


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
  # load trips and timetables, filtered by the desired route
  trips = _LoadTripsForRoute(desired_route_id)
  timetable = _LoadTimetableForTrips(set(trips.keys()))
  # print all the trips, sorted by start time, only for the interesting stops
  trip_starts = _GetTripStarts(trips, timetable)
  for trip_start in sorted(trip_starts.keys()):
    for trip_id in sorted(trip_starts[trip_start]):
      print('TRIP %s @ %s' % (trip_id, trip_start.strftime('%H:%M')))
      for int_stop_sequence, stop_id, arrival_time in timetable[trip_id]:
        if stop_id in interesting_stops_ids:
          print('    %d: %s @ %s' % (
              int_stop_sequence, stops[stop_id], arrival_time.strftime('%H:%M')))
  logging.info('DONE')


# only execute main() if used directly --- not sure how robust this is...
if __name__ == '__main__':
  sys.exit(main(sys.argv))
