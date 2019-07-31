#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  mdpactors.py
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

import zmq.green as zmq
import socket
import abc

pickle = None
try:
    import cPickle as pickle #Python 2
except ImportError:
    import _pickle as pickle #Python 3

from gevent.queue import Queue

from compysition.actor import Actor

DEFAULT_PORT = 9000

#TODO: Will be simple to implement ZMQDealer, ZMQREQ, ZMQREP, but the abstract bases may morph during implementations


class _ZMQContextSharer(abc.ABCMeta):

    """
    This metaclass is designed to hold a shared context across multiple instances for use by the ZeroMQ 'inproc' protocol.
    As inproc will only work inside of a single interpretor instance, so to will this shared_context
    """

    def __init__(cls, *args, **kwargs):
        if not hasattr(cls, 'shared_context'):
            cls.shared_context = zmq.Context()

        super(_ZMQContextSharer, cls).__init__(*args, **kwargs)

class _ZMQ(Actor):

    """
    **Abstract base for ZMQ objects**

    Parameters:
        name (str):
            | The instance name
        port (Optional[int]):
            | The port for the ZeroMQ socket to connect or bind to
            | Default: 9000
        host (Optional[str]):
            | The host for the ZeroMQ socket to connect or bind to
            | Default: Attempts to resolve local host name via socket.gethostbyname(socket.gethostname())
        mode (Optional[str]):
            | The mode for the socket to use. (bind|connect)
            | Default: connect

    Abstract Properties:
        protocol (zmq.PROTOCOL)
            | The ZermMQ protocol for an implementing class to connect with.
            | (zmq.PUSH, zmq.PULL, zmq.DEALER, zmq.ROUTER, zmq.REQ, zmq.REP, zmq.SUB, zmq.PUB, etc)

    """

    TCP = "tcp"
    IPC = "ipc"
    INPROC = "inproc"

    __metaclass__ = _ZMQContextSharer

    @abc.abstractproperty
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, protocol):
        self._protocol = protocol

    def __init__(self, name, port=DEFAULT_PORT, transmission_protocol=TCP, socket_file=None, host=None, mode="connect", alt_sockets="", socket_name=None, *args, **kwargs):
        super(_ZMQ, self).__init__(name, *args, **kwargs)
        self.blockdiag_config["shape"] = "cloud"
        self.port = port
        self.host = host or socket.gethostbyname(socket.gethostname())
        self.mode = mode
        self.socket_name = socket_name

        socket_configs = self._get_alt_socket_configs(alt_sockets=alt_sockets)
        if self.mode == "bind":
            if socket_name is None:
                self.socket_map = {
                    None: self._create_socket(port=self.port, transmission_protocol=transmission_protocol, socket_file=socket_file)
                }
            elif self.socket_name is not None:
                self.socket_map = {
                    self.socket_name: self._create_socket(port=socket_configs[self.socket_name][0], transmission_protocol=transmission_protocol, socket_file=socket_configs[self.socket_name][1])
                }
        elif self.mode == "connect":
            self.socket_map = {
                None: self._create_socket(port=self.port, transmission_protocol=transmission_protocol, socket_file=socket_file)
            }
            for socket_name, socket_config in socket_configs.items():
                self.socket_map[socket_name] = self._create_socket(port=socket_config[0], transmission_protocol=transmission_protocol, socket_file=socket_config[1])

    def _get_alt_socket_configs(self, alt_sockets):
        socket_configs = {}
        alt_sockets = alt_sockets.split(",")
        for alt_socket in alt_sockets:
            try:
                alt_configs = alt_socket.split(":")
                alt_name = alt_configs[0]
                alt_port = alt_configs[1]
                try:
                    alt_file = alt_configs[2]
                except IndexError:
                    alt_file = None
                socket_configs[alt_name] = (alt_port, alt_file)
            except Exception:
                pass
        return socket_configs

    def _create_socket(self, port, transmission_protocol, socket_file):
        format_connection = {
            self.TCP: "tcp://{0}:{1}".format(self.host, port),
            self.IPC: "ipc://{0}".format(socket_file),
            self.INPROC: "inproc://{0}".format(socket_file)
        }
        if transmission_protocol in format_connection.keys():
            transmission_protocol = transmission_protocol
        else:
            raise ValueError("Transmission protocol must be in {0}".format(self.format_connection.keys()))

        return self.create_socket(format_connection=format_connection, transmission_protocol=transmission_protocol)

    def create_socket(self, format_connection, transmission_protocol):
        context = self.shared_context if transmission_protocol == self.INPROC else zmq.Context()
        context = context or zmq.Context()
        _socket = context.socket(self.protocol)

        if self.mode == "connect":
            _socket.connect(format_connection[transmission_protocol])
        elif self.mode == "bind":
            _socket.bind(format_connection[transmission_protocol])

        return _socket


class _ZMQOut(_ZMQ):

    """
    **A still-abstract implementation of _ZMQ base that is designed for an event being SENT over ZeroMQ**
    """

    def __init__(self, name, mode="connect", *args, **kwargs):
        super(_ZMQOut, self).__init__(name, mode=mode, *args, **kwargs)
        self.outbound_queue = Queue()

    def consume(self, event, *args, **kwargs):
        self.outbound_queue.put(event)

    def pre_hook(self):
        self.threads.spawn(self.__consume_outbound_queue)

    def __consume_outbound_queue(self):
        while self.loop():
            try:
                event = self.outbound_queue.get(timeout=2.5)
            except Exception:
                event = None

            if event is not None:
                socket = self.socket_map.get(event.get("zmq_socket_name", None), None)
                if socket is not None:
                    try:
                        socket.send(pickle.dumps(event))
                    except Exception as err:
                        print(str(event), event.__class__.__name__)
                        self.logger.error("Unable to send event over ZMQ: {err}".format(err=err), event=event)


class _ZMQIn(_ZMQ):

    """
    **A still-abstract implementation of _ZMQ base that is designed for an event being RECEIVED over ZeroMQ**
    """

    def __init__(self, name, mode="bind", socket_name=None, *args, **kwargs):
        super(_ZMQIn, self).__init__(name, mode=mode, socket_name=socket_name, *args, **kwargs)
        self.poller = zmq.Poller()
        self.socket = self.socket_map.get(socket_name)
        self.poller.register(self.socket, zmq.POLLIN)

    def pre_hook(self):
        self.threads.spawn(self._listen)

    def consume(self, event, *args, **kwargs):
        self.logger.warning("Received event on queue, but this actor does not consume. Event has been discarded", event=event)

    def _listen(self):
        while self.loop():
            try:
                items = self.poller.poll()
            except KeyboardInterrupt:
                break

            if items:
                event = self.socket.recv_multipart()
                event = pickle.loads(event[0])
                self.send_event(event)


class ZMQPush(_ZMQOut):

    """
    **Send events over ZMQ Push**
    """

    protocol = zmq.PUSH


class ZMQPull(_ZMQIn):

    """
    **Receive events over ZMQ Pull**
    """

    protocol = zmq.PULL
