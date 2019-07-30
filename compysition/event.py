#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  default.py
#
#  Copyright 2014 Adam Fiebig <fiebig.adam@gmail.com>
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
import json
import traceback
import re
import xmltodict

from uuid import uuid4 as uuid
from lxml import etree
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from collections import OrderedDict, defaultdict
from xml.parsers import expat
from xml.sax.saxutils import XMLGenerator

from .errors import (ResourceNotModified, MalformedEventData, InvalidEventDataModification, UnauthorizedEvent,
    ForbiddenEvent, ResourceNotFound, EventCommandNotAllowed, ActorTimeout, ResourceConflict, ResourceGone,
    UnprocessableEventData, EventRateExceeded, CompysitionException, ServiceUnavailable)

"""
Compysition event is created and passed by reference among actors
"""

NoneType = type(None)
DEFAULT_EVENT_SERVICE = "default"
DEFAULT = object()


def internal_xmlify(_json):
    if isinstance(_json, list) or len(_json) > 1:
        _json = {"jsonified_envelope": _json}
    _, value = next(_json.iteritems())
    if isinstance(value, list):
        _json = {"jsonified_envelope": _json}
    return _json


def remove_internal_xmlify(_json):
    if len(_json) == 1 and isinstance(_json, dict):
        key, value = next(_json.iteritems())
        if key == "jsonified_envelope":
            _json = value
    return _json

class NullLookupValue(object):

    def get(self, key, value=None):
        return self


class UnescapedDictXMLGenerator(XMLGenerator):
    """
    Simple class designed to enable the use of an unescaped functionality
    in the event that dictionary value data is already XML
    """

    def characters(self, content):
        try:
            if content.lstrip().startswith("<"):
                etree.fromstring(content)
                self._write(content)
            else:
                XMLGenerator.characters(self, content)
        except (AttributeError, Exception):
            #TODO could be more specific on errors caught
            XMLGenerator.characters(self, content)

setattr(xmltodict, "XMLGenerator", UnescapedDictXMLGenerator)
_XML_TYPES = [etree._Element, etree._ElementTree, etree._XSLTResultTree]
_JSON_TYPES = [dict, list, OrderedDict]


class DataFormatInterface(object):
    """
    Interface used as an identifier for data format classes. Used during event type conversion
    To create a new datatype, simply implement this interface on the newly created class
    """
    pass


class Event(object):

    """
    Anatomy of an event:
        - event_id: The unique and functionally immutable ID for this new event
        - meta_id:  The ID associated with other unique event data flows. This ID is used in logging
        - service:  (default: default) Used for compatability with the ZeroMQ MajorDomo configuration. Scope this to specific types of interprocess routing
        - data:     <The data passed and worked on from event to event. Mutable and variable>
        - kwargs:   All other kwargs passed upon Event instantiation will be added to the event dictionary

    """

    _content_type = "text/plain"

    def __init__(self, meta_id=None, service=None, data=None, *args, **kwargs):
        self.event_id = uuid().get_hex()
        self.meta_id = meta_id if meta_id else self.event_id
        self.service = service or DEFAULT_EVENT_SERVICE
        self.data = data
        self.error = None
        self.created = datetime.now()
        self.__dict__.update(kwargs)

    def set(self, key, value):
        try:
            setattr(self, key, value)
            return True
        except Exception:
            return False

    def get(self, key, default=DEFAULT):
        val = getattr(self, key, default)
        if val is DEFAULT:
            raise AttributeError("Event property '{property}' does not exist".format(property=key))
        return val

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        try:
            self._data = self.conversion_methods[data.__class__](data)
        except KeyError:
            raise InvalidEventDataModification("Data of type '{_type}' was not valid for event type {cls}: {err}".format(_type=type(data),
                                                                                          cls=self.__class__, err=traceback.format_exc()))
        except ValueError as err:
            raise InvalidEventDataModification("Malformed data: {err}".format(err=err))
        except Exception as err:
            raise InvalidEventDataModification("Unknown error occurred on modification: {err}".format(err=err))

    @property
    def event_id(self):
        return self._event_id

    @event_id.setter
    def event_id(self, event_id):
        if self.get("_event_id", None):
            raise InvalidEventDataModification("Cannot alter event_id once it has been set. A new event must be created")
        else:
            self._event_id = event_id

    def _list_get(self, obj, key, default):
        try:
            return obj[int(key)]
        except (ValueError, IndexError, TypeError, KeyError, AttributeError) as e:
            return default

    def _getattr(self, obj, key, default):
        try:
            return getattr(obj, key, default)
        except TypeError as e:
            return default

    def _obj_get(self, obj, key, default):
        try:
            return obj.get(key, default)
        except (TypeError, AttributeError) as e:
            return default

    def lookup(self, path):
        """
        Implements the retrieval of a single list index through an integer path entry
        """
        if isinstance(path, str):
            path = [path]

        value = reduce(lambda obj, key: self._obj_get(obj, key, self._getattr(obj, key, self._list_get(obj, key, NullLookupValue()))), [self] + path)
        if isinstance(value, NullLookupValue):
            value = None
        return value

    def get_properties(self):
        """
        Gets a dictionary of all event properties except for event.data
        Useful when event data is too large to copy in a performant manner
        """
        return {k: v for k, v in self.__dict__.iteritems() if k != "data" and k != "_data"}

    def __getstate__(self):
        return dict(self.__dict__)

    def __setstate__(self, state):
        self.__dict__ = state
        self.data = state['_data']
        self.error = state.get('_error', None)

    def __str__(self):
        return str(self.__getstate__())

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, exception):
        self._set_error(exception)

    def _set_error(self, exception):
        self._error = exception

    conversion_methods = defaultdict(lambda: lambda data: data)

    def format_error(self):
        if self.error:
            if hasattr(self.error, 'override') and self.error.override:
                return self.error.override
            else:
                messages = self.error.message
                if not isinstance(messages, list):
                    messages = [messages]
                errors = map(lambda _error:
                             dict(message=str(getattr(_error, "message", _error)), **self.error.__dict__),
                             messages)
                return errors
        else:
            return None

    def error_string(self):
        if self.error:
            return str(self.format_error())
        else:
            return None

    def data_string(self):
        return str(self.data)

    def convert(self, convert_to):
        if issubclass(convert_to, self.__class__):
            # Widening conversion
            new_class = convert_to
        else:
            if not issubclass(self.__class__, convert_to):
                # A complex widening conversion
                bases = tuple([convert_to] + filter(lambda cls: not issubclass(cls, DataFormatInterface) and not issubclass(convert_to, cls), list(self.__class__.__bases__) + [self.__class__]))
                if len(bases) == 1:
                    new_class = bases[0]
                else:
                    new_class = filter(lambda cls: cls.__bases__ == bases, built_classes)[0]
            else:
                # This is an attempted narrowing conversion
                raise InvalidEventConversion("Narrowing event conversion attempted, this is not allowed <Attempted {old} -> {new}>".format(
                        old=self.__class__, new=convert_to))

        new_class = new_class.__new__(new_class)
        new_class.__dict__.update(self.__dict__)
        new_class.data = self.data
        return new_class

    def clone(self):
        return deepcopy(self)


class HttpEvent(Event):

    content_type = "text/plain"

    def __init__(self, headers=None, status=(200, "OK"), environment={}, pagination=None, *args, **kwargs):
        self.headers = headers or {}
        self.method = environment.get('REQUEST_METHOD', None)
        self.status = status
        self.environment = environment
        self._pagination = pagination
        super(HttpEvent, self).__init__(*args, **kwargs)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        if isinstance(status, tuple):
            self._status = status
        elif isinstance(status, str):
            status = re.split(' |-', status, 1)
            if len(status) == 2:
                self._status = (int(status[0]), status[1])

    def _set_error(self, exception):
        if isinstance(exception, Exception):
            error_state = http_code_map[exception.__class__]
            self.status = error_state.get("status")
            self.headers.update(error_state.get("headers", {}))

        super(HttpEvent, self)._set_error(exception)

    @property
    def pagination(self):
        return self._pagination

    @pagination.setter
    def pagination(self, pagination_dict):
        if 'limit' in pagination_dict and 'offset' in pagination_dict:
            self._pagination = pagination_dict
        else:
            raise ValueError('Must have limit and offset keys set in pagination_dict')



class _XMLFormatInterface(DataFormatInterface):

    content_type = "application/xml"

    conversion_methods = {str: lambda data: etree.fromstring(data)}
    conversion_methods.update(dict.fromkeys(_XML_TYPES, lambda data: data))
    conversion_methods.update(dict.fromkeys(_JSON_TYPES, lambda data: etree.fromstring(xmltodict.unparse(internal_xmlify(data)).encode('utf-8'))))
    conversion_methods.update({None.__class__: lambda data: etree.fromstring("<root/>")})

    def __getstate__(self):
        state = super(_XMLFormatInterface, self).__getstate__()
        state['_data'] = etree.tostring(self.data)
        return state

    def data_string(self):
        return etree.tostring(self.data)

    def format_error(self):
        errors = super(_XMLFormatInterface, self).format_error()
        if self.error and self.error.override:
            try:
                result = etree.fromstring(errors)
            except Exception:
                result = errors
        elif errors:
            result = etree.Element("errors")
            for error in errors:
                error_element = etree.Element("error")
                message_element = etree.Element("message")
                code_element = etree.Element("code")
                error_element.append(message_element)
                message_element.text = error['message']
                if getattr(error, 'code', None) or ('code' in error and error['code']):
                    code_element.text = error['code']
                    error_element.append(code_element)
                result.append(error_element)
        return result

    def error_string(self):
        error = self.format_error()
        if error is not None:
            try:
                error = etree.tostring(error, pretty_print=True)
            except Exception:
                pass
        return error


def decimal_default(obj):
    """ Lets us pass around Decimal type objects and not have to worry about doing conversions in the individual actors"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def should_force_list(path, key, value):
    """
    This is a callback passed to xmltodict.parse which checks for an xml attribute force_list in the XML, and if present
    the outputted JSON is an list, regardless of the number of XML elements. The default behavior of xmltodict is that
    if there is only one <element>, to output as a dict, but if there are multiple then to output as a list. @force_list
    is useful in situations where the xml could have 1 or more <element>'s, but code handling the JSON elsewhere expects
    a list.

    For example, if you have an XSLT that produces 1 or more items, you could have the resulting output of the XSLT
    transformation be (note we only need to set force_list on the first node, the rest will automatically be appended):
        <xsl:if test="position() = 1">
            <transactions force_list=True>
                <amount>85</amount>
            </transactions>
        </xsl:if>

    and the JSON output will be:
        {"transactions" : [
            "amount": "85"
            ]
        }
    whereas the default when the @force_list attribute is not present would be:
        {"transactions": {
            "amount": "85"
            }
        }

    which could break code handling this JSON which always expects "transactions" to be a list.
    """
    if isinstance(value, dict) and '@force_list' in value:
        # pop attribute off so that it doesn't get included in the response
        value.pop('@force_list')
        return True
    else:
        return False


class _JSONFormatInterface(DataFormatInterface):

    content_type = "application/json"

    conversion_methods = {str: lambda data: json.loads(data)}
    conversion_methods.update(dict.fromkeys(_JSON_TYPES, lambda data: json.loads(json.dumps(data, default=decimal_default))))
    conversion_methods.update(dict.fromkeys(_XML_TYPES, lambda data: remove_internal_xmlify(xmltodict.parse(etree.tostring(data), expat=expat, force_list=should_force_list))))
    conversion_methods.update({None.__class__: lambda data: {}})

    def __getstate__(self):
        state = super(_JSONFormatInterface, self).__getstate__()
        state['_data'] = json.dumps(self.data)
        return state

    def data_string(self):
        return json.dumps(self.data, default=decimal_default)

    def error_string(self):
        error = self.format_error()
        if error:
            try:
                error = json.dumps(error)
            except Exception:
                pass
        return error


class XMLEvent(_XMLFormatInterface, Event):
    pass


class JSONEvent(_JSONFormatInterface, Event):
    pass


class JSONHttpEvent(JSONEvent, HttpEvent):
    pass


class XMLHttpEvent(XMLEvent, HttpEvent):
    pass


class LogEvent(Event):
    """
    This is a lightweight event designed to mimic some of the event properties of a regular event
    """

    def __init__(self, level, origin_actor, message, id=None, sensitive=False):
        self.id = id
        self.event_id = uuid().get_hex()
        self.meta_id = id or self.event_id
        self.level = level
        self.time = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
        self.origin_actor = origin_actor
        self.message = message
        self.sensitive = sensitive
        self.data = {"id":              self.id,
                    "level":            self.level,
                    "time":             self.time,
                    "origin_actor":     self.origin_actor,
                    "message":          self.message}

built_classes = [Event, XMLEvent, JSONEvent, HttpEvent, JSONHttpEvent, XMLHttpEvent, LogEvent]
__all__ = map(lambda cls: cls.__name__, built_classes)

http_code_map = defaultdict(lambda: {"status": ((500, "Internal Server Error"))},
                            {
                                ResourceNotModified:    {"status": (304, "Not Modified")},
                                MalformedEventData:     {"status": (400, "Bad Request")},
                                InvalidEventDataModification: {"status": (400, "Bad Request")},
                                UnauthorizedEvent:      {"status": (401, "Unauthorized"),
                                                         "headers": {'WWW-Authenticate': 'Basic realm="Compysition Server"'}},
                                ForbiddenEvent:         {"status": (403, "Forbidden")},
                                ResourceNotFound:       {"status": (404, "Not Found")},
                                EventCommandNotAllowed: {"status": (405, "Method Not Allowed")},
                                ActorTimeout:           {"status": (408, "Request Timeout")},
                                ResourceConflict:       {"status": (409, "Conflict")},
                                ResourceGone:           {"status": (410, "Gone")},
                                UnprocessableEventData: {"status": (422, "Unprocessable Entity")},
                                EventRateExceeded:      {"status": (429, "Too Many Requests")},
                                CompysitionException:   {"status": (500, "Internal Server Error")},
                                ServiceUnavailable:     {"status": (503, "Service Unavailable")},
                                NoneType:               {"status": (200, "OK")}         # Clear an error
                            })

__all__.append(http_code_map)