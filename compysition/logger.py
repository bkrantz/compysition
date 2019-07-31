#!/usr/bin/env python
#
# -*- coding: utf-8 -*-
#
#  logging.py
#
#  Copyright 2014 Adam Fiebig <fiebig.adam@gmail.com>
#  Originally based on "wishbone" project by smetj
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#

import logging

from compysition.queue import _InternalQueuePool
from compysition.event import LogEvent, Event
from compysition.errors import QueueFull

class Logger(object):

    """**Generates Compysition formatted log messages following the python priority definition to a pool of queues**

    We use a pool in order to support multiple logging types per process. For example, sending to a third party log
    aggregator as WELL as using a filelogger

    Args:
        - name(str):
            | The name to use when sending log events
        - queue_pool(_InternalQueuePool):
            | The pool to use when sending log events
    """

    def __init__(self, name, queue_pool):
        self.name = name
        if not isinstance(queue_pool, _InternalQueuePool):
            raise TypeError("Logger queue_pool must be of type '_InternalQueuePool'")

        self.__pool = queue_pool

    def log(self, level, message, event=None, log_entry_id=None, sensitive=False):
        """
        Uses log_entry_id explicitely as the logged ID, if defined. Otherwise, will attempt to ascertain the ID from 'event', if passed
        """
        if not log_entry_id:
            if event:
                log_entry_id = event.meta_id

        for key in self.__pool.iterkeys():
            try:
                log_event = LogEvent(level, self.name, message, id=log_entry_id, sensitive=sensitive)
                self.__pool[key].put(log_event)
            except QueueFull:
                self.__pool.wait_until_free()
                self.__pool[key].put(log_event)

    def critical(self, message, event=None, log_entry_id=None, sensitive=False):
        """Generates a log message with priority logging.CRITICAL
        """
        self.log(logging.CRITICAL, message, event=event, log_entry_id=log_entry_id, sensitive=sensitive)

    def error(self, message, event=None, log_entry_id=None, sensitive=False):
        """Generates a log message with priority error(3).
        """
        self.log(logging.ERROR, message, event=event, log_entry_id=log_entry_id, sensitive=sensitive)

    def warn(self, message, event=None, log_entry_id=None, sensitive=False):
        """Generates a log message with priority logging.WARN
        """
        self.log(logging.WARN, message, event=event, log_entry_id=log_entry_id, sensitive=sensitive)
    warning=warn

    def info(self, message, event=None, log_entry_id=None, sensitive=False):
        """Generates a log message with priority logging.INFO.
        """
        self.log(logging.INFO, message, event=event, log_entry_id=log_entry_id, sensitive=sensitive)

    def debug(self, message, event=None, log_entry_id=None, sensitive=False):
        """Generates a log message with priority logging.DEBUG
        """
        self.log(logging.DEBUG, message, event=event, log_entry_id=log_entry_id, sensitive=sensitive)

    def alt_logger(self, alt_name, event, **kwargs):
        new_event = Event()
        new_event._event_id, new_event.meta_id, new_event.service = event._event_id, event.meta_id, event.service
        prefix = "%s_" % alt_name
        for key, value in kwargs.items():
            field_name = "%s%s" % (prefix,key)
            new_event.set(field_name, value)
        new_event.set("zmq_socket_name", alt_name)
        for key in self.__pool.iterkeys():
            try:
                self.__pool[key].put(new_event)
            except QueueFull:
                self.__pool.wait_until_free()
                self.__pool[key].put(new_event)