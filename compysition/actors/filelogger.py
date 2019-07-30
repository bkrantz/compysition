#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#       stdout.py
#
#       Copyright 2014 Adam Fiebig fiebig.adam@gmail.com
#
#       This program is free software; you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation; either version 3 of the License, or
#       (at your option) any later version.
#
#       This program is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#       GNU General Public License for more details.
#
#       You should have received a copy of the GNU General Public License
#       along with this program; if not, write to the Free Software
#       Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#       MA 02110-1301, USA.
#
#

import logging
import logging.handlers
import traceback
import os
import gevent.lock

from compysition.actor import Actor
from compysition.event import LogEvent

class RotatingFileHandler(logging.handlers.RotatingFileHandler):

    def __init__(self, file_path, *args, **kwargs):
        self.make_file(file_path)
        super(RotatingFileHandler, self).__init__(file_path, *args, **kwargs)

    def createLock(self):
        """Set self.lock to a new gevent RLock.
        """
        self.lock = gevent.lock.RLock()

    def make_file(self, file_path):
        file_dir = os.path.dirname(file_path)
        if not os.path.isfile(file_path):
            if not os.path.exists(file_dir):
                self.make_file_dir(file_dir)
            with open(file_path, 'w+'):
                pass

    def make_file_dir(self, file_path):
        sub_path = os.path.dirname(os.path.abspath(file_path))
        if not os.path.exists(sub_path):
            self.make_file_dir(sub_path)
        if not os.path.exists(file_path):
            os.mkdir(file_path)


class FileLogger(Actor):
    '''**Prints incoming events to a log file for debugging.**
    '''

    input = LogEvent

    def __init__(self, name, default_filename="compysition.log", level="INFO", directory="logs", maxBytes=20000000, backupCount=10, ignore_sensitive=False, *args, **kwargs):
        super(FileLogger, self).__init__(name, *args, **kwargs)
        self.blockdiag_config["shape"] = "note"
        self.default_filename = default_filename
        self.level = getattr(logging, level.upper(), logging.INFO)
        self.directory = directory
        self.maxBytes = int(maxBytes)
        self.backupCount = int(backupCount)
        self.ignore_sensitive = ignore_sensitive
        self.loggers = {}

    def _create_logger(self, filepath):
        file_logger = logging.getLogger(filepath)
        logHandler = RotatingFileHandler(r'{0}'.format(filepath), maxBytes=self.maxBytes, backupCount=self.backupCount)
        logFormatter = logging.Formatter('%(message)s') # We will do ALL formatting ourselves in qlogger, as we want to be extremely literal to make sure the timestamp
                                                        # is generated at the time that logger.log was invoked, not the time it was written to file
        logHandler.setFormatter(logFormatter)
        file_logger.addHandler(logHandler)
        file_logger.setLevel(self.level)
        return file_logger

    def _process_log_entry(self, event):
        if not self.ignore_sensitive or not event.get("sensitive", False):
            event_filename = event.get("logger_filename", self.default_filename)
            logger = self.loggers.get(event_filename, None)
            if not logger:
                logger = self._create_logger("{0}/{1}".format(self.directory, event_filename))
                self.loggers[event_filename] = logger

            self._do_log(logger, event)

    def _do_log(self, logger, event):
        actor_name = event.origin_actor
        id = event.id
        message = event.message
        level = event.level
        time = event.time

        entry_prefix = "{0} {1} ".format(time, logging._levelNames.get(level)) # Use the time from the logging invocation as the timestamp

        if id:
            entry = "actor={0}, id={1} :: {2}".format(actor_name, id, message)
        else:
            entry = "actor={0} :: {1}".format(actor_name, message)

        try:
            logger.log(level, "{0}{1}".format(entry_prefix, entry))
        except Exception:
            print(traceback.format_exc())

    def consume(self, event, *args, **kwargs):
        self._process_log_entry(event)
