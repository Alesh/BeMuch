""" This module help to build multiprocessing tornado apps.
"""
import uuid
import queue
import pickle
import signal
import logging
import multiprocessing
from time import time as now
from functools import partial
from tornado.ioloop import IOLoop
from tornado.concurrent import Future

MAIN = '$$BEMUCH$$MAIN$$'
CALL = '$$BEMUCH$$CALL$$'
RESULT = '$$BEMUCH$$RESULT$$'
EXCEPTION = '$$BEMUCH$$EXCEPTION$$'
TERMINATE = '$$BEMUCH$$TERMINATE$$'


class _EndPoint(object):

    """ Endpoint of interprocess communication.
    """

    all = dict()

    def __init__(self, router, endpoint, app_factory=None, channel=None):
        self.app = None
        self.router = router
        self.endpoint = endpoint
        self.app_factory = app_factory
        self.channel = channel

    def send(self, endpoint, message):
        """ Sends msg to recipient with given endpoint.
        """
        try:
            self.router.put((endpoint, message), block=False)
        except queue.Full:
            logging.error("Cannot send message to endpoint '{}',"
                          " router query is full".format(endpoint))

    def call(self, endpoint, method, *args, **kwargs):
        """ Interprocess call.
        """
        future = Future()
        future_uuid = str(uuid.uuid4())
        self.futures[future_uuid] = future
        self.send(endpoint,
                  (CALL, method,
                   pickle.dumps((args, kwargs)),
                   (self.endpoint, future_uuid)))
        return future

    def _run(self, ioloop):
        """ Calls from subprocess when it is starting.
        """
        self.ioloop = ioloop
        self.futures = dict()
        if self.app_factory is not None:
            if self.channel is not None:
                self.ioloop.add_handler(self.channel._reader.fileno(),
                                        self._message_received, IOLoop.READ)
            self.app = self.app_factory(ioloop=self.ioloop)
            self.app.bemuch = self

    def _message_received(self, *args):
        if self.channel is not None:
            message = self.channel.get(block=False)
            self._process_message(message)

    def _process_message(self, message):
        type, *fields = message
        if type == CALL:
            method, pickled, (endpoint, future_uuid) = fields
            args, kwargs = pickle.loads(pickled)
            app_method = getattr(self.app, method, None)

            # for coroutines using below callback for return result
            def done_callback(future):
                try:
                    result = future.result()
                    self.send(endpoint, (RESULT,
                                         pickle.dumps(result),
                                         future_uuid))
                except Exception as exc:
                    self.send(endpoint, (EXCEPTION,
                                         pickle.dumps(exc),
                                         future_uuid))

            try:
                if app_method is not None:
                    result = app_method(*args, **kwargs)
                    if isinstance(result, Future):
                        result.add_done_callback(done_callback)
                    else:
                        self.send(endpoint, (RESULT,
                                             pickle.dumps(result),
                                             future_uuid))
                else:
                    raise AttributeError("'{}' has no attribute '{}'"
                                         "".format(self.app.__class__,
                                                   method))
            except Exception as exc:
                self.send(endpoint, (EXCEPTION,
                                     pickle.dumps(exc),
                                     future_uuid))
        elif type in (RESULT, EXCEPTION):
            pickled, future_uuid = fields
            future = self.futures.pop(future_uuid)
            if type == RESULT:
                future.set_result(pickle.loads(pickled))
            else:
                future.set_exception(pickle.loads(pickled))
        else:
            def on_message(message):
                logging.warning("Endpoint `{}` got message"
                                " but nothing to does.\nmessages: {}"
                                "".format(self.endpoint, message))
            if self.app is not None:
                getattr(self.app, 'on_message', on_message)(message)
            else:
                on_message(message)


class _SubProcess(_EndPoint):

    """ BeMuch subprocess.
    """

    def __init__(self, router, endpoint, app_factory):
        channel = multiprocessing.Queue()
        super(_SubProcess, self).__init__(router, endpoint,
                                          app_factory, channel)

    def run(self):
        ioloop = IOLoop()
        self._run(ioloop)
        getattr(self.app, 'on_start', lambda: None)()
        self.ioloop.start()

    def _process_message(self, message):
        type, *fields = message
        if type == TERMINATE:
            logging.warning("Subprocess with endpoint `{}`"
                            " is terminating".format(self.endpoint))
            getattr(self.app, 'on_terminate', lambda: None)()
            self.ioloop.stop()
        else:
            super(_SubProcess, self)._process_message(message)


class _Controller(_EndPoint):

    """ BeMuch controler.
    """

    def __init__(self, restart_if_fail, on_terminated, ioloop,
                 terminator_signals, wait_terminating_for):
        self.ioloop = ioloop
        self.terminating = False
        self.subprocesses = dict()
        router = multiprocessing.Queue()
        self.on_terminated = on_terminated
        self.restart_if_fail = restart_if_fail
        self.terminator_signals = terminator_signals
        self.wait_terminating_for = wait_terminating_for
        super(_Controller, self).__init__(router, MAIN)

        def handle_signal(sig, frame):
            self.ioloop.add_callback_from_signal(self.terminate)

        for signum in self.terminator_signals:
            signal.signal(signum, handle_signal)

    def run(self, ioloop=None):
        """ Starts all subprocesses and event loop.
        """
        def sub_terminated(endpoint, sentinel, *args):
            self.ioloop.remove_handler(sentinel)
            del self.subprocesses[endpoint]
            self.on_terminated(endpoint)

        endpoints = tuple(_EndPoint.all.keys())
        if endpoints:
            for endpoint in endpoints:
                app_factory = _EndPoint.all.pop(endpoint)
                sub = _SubProcess(self.router, endpoint, app_factory)
                sub.process = multiprocessing.Process(target=sub.run)
                sub.process.start()
                if sub.process.is_alive():
                    self.subprocesses[endpoint] = sub
                    callback = partial(sub_terminated, endpoint)
                    self.ioloop.add_handler(sub.process.sentinel,
                                            callback, IOLoop.READ)
                else:
                    logging.error("Cannot start subprocess for endpoint '{}'"
                                  "".format(endpoint))

            self.ioloop.add_handler(self.router._reader.fileno(),
                                    self._message_received, IOLoop.READ)
            self._run(self.ioloop)
            self.ioloop.start()

    def terminate(self, cnt=0):
        self.terminating = True
        endpoints = tuple(self.subprocesses.keys())
        for endpoint in endpoints:
            self.send(endpoint, (TERMINATE,))
        if endpoints and cnt < self.wait_terminating_for * 10:
            self.ioloop.add_timeout(now()+0.1, self.terminate, cnt+1)
        else:
            for endpoint in endpoints:
                sub = self.subprocesses[endpoint]
                if sub.process.is_alive():
                    sub.process.terminate()
            self.ioloop.stop()

    def _message_received(self, *args):
        endpoint, message = self.router.get(block=False)
        if endpoint in (MAIN, self.endpoint):
            self._process_message(message)
        elif endpoint in self.subprocesses:
            try:
                self.subprocesses[endpoint].channel.put(message, block=False)
            except queue.Full:
                logging.warning("Cannot send message to endpoint '{}',"
                                " endpoint query is full".format(endpoint))


def startApp(endpoint, app_class, *args, **kwargs):
    """ Registers application class and prepare to start
    application instance into subprocess.
    """
    _EndPoint.all[endpoint] = partial(app_class, *args, **kwargs)


def start(ioloop=IOLoop.instance(),
          restart_if_fail=False, on_terminated=lambda endpoint: None,
          terminator_signals=[signal.SIGINT, signal.SIGTERM],
          wait_terminating_for=3):
    """ Starts all subprocesses and event loop.
    """
    _Controller(restart_if_fail, on_terminated, ioloop,
                terminator_signals, wait_terminating_for).run()
