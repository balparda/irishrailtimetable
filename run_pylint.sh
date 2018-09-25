#!/bin/bash
#
# Copyright (C) 2018 Daniel Balparda de Carvalho (balparda@gmail.com).
# This file is part of Irish Rail Timetable.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see http://www.gnu.org/licenses/gpl-3.0.txt.
#

pylint3 -r no --disable bad-indentation,invalid-name,bad-continuation,locally-disabled,locally-enabled,duplicate-code,not-callable,too-many-statements,too-many-nested-blocks,too-few-public-methods,too-many-lines,too-many-instance-attributes,too-many-arguments,too-many-locals,too-many-return-statements,too-many-branches,consider-iterating-dictionary *.py
