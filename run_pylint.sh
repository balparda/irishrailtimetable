#!/bin/bash
#
# Copyright 2018 Daniel Balparda (balparda@gmail.com)
#
# also: --disable locally-disabled,duplicate-code

pylint3 -r no --disable bad-indentation,invalid-name,bad-continuation,locally-disabled,locally-enabled,duplicate-code,not-callable,too-many-statements,too-few-public-methods,too-many-lines,too-many-instance-attributes,too-many-arguments,too-many-locals,too-many-return-statements,too-many-branches,consider-iterating-dictionary *.py
