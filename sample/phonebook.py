import logging
import tornado.web
import tornado.ioloop
from tornado.gen import coroutine
from tornado.util import ObjectDict
from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer
import bemuch


class FBItem(ObjectDict):

    def __init__(self, first_name, last_name):
        super(FBItem, self).__init__({
            'first_name': first_name,
            'last_name': last_name})

    @classmethod
    def make(cls, rh):
        return cls(rh.get_argument("first_name"),
                   rh.get_argument("last_name"))


class RestHandler(tornado.web.RequestHandler):

    @coroutine
    def get(self):
        data = yield self.application.all()
        self.write(data)

    @coroutine
    def post(self):
        phone = self.get_argument("phone")
        yield self.application.update(phone, FBItem.make(self))


class RestHandlerA(tornado.web.RequestHandler):

    @coroutine
    def get(self, phone):
        data = yield self.application.read(phone)
        self.write(data)

    @coroutine
    def put(self, phone):
        yield self.application.update(phone, FBItem.make(self))

    @coroutine
    def delete(self, phone):
        yield self.application.delete(phone)


class CrudApplication(tornado.web.Application):

    def __init__(self, port, selector, ioloop=None):
        self.port = port
        self.phonebook = dict()
        tornado.web.Application.__init__(self, [
            (r"/phonebook", RestHandler),
            (r"/phonebook/(\+*[0-9]+)", RestHandlerA)
        ])
        self.port = port
        self.selector = selector
        self.httpd = HTTPServer(self, io_loop=ioloop or IOLoop.instance())
        self.httpd.listen(self.port)

    def on_start(self):
        logging.info("Starting phonebook sharded"
                     " instance {} on http://localhost:{}/phonebook"
                     "".format(self.bemuch.endpoint, self.port))

    def _all(self):
        result = list()
        for phone in self.phonebook:
            item = self.phonebook[phone]
            item['phone'] = phone
            result.append(item)
        return result

    @coroutine
    def all(self):
        endpoints = set()
        for n in range(0, 64):
            endpoints.add(self.selector(n))
        result = list()
        for endpoint in tuple(endpoints):
            if endpoint == self.bemuch.endpoint:
                data = self._all()
            else:
                data = yield self.bemuch.call(endpoint, '_all')
            result.extend(data)
        return {'phonebook': result}

    @coroutine
    def read(self, phone):
        endpoint = self.selector(phone)
        if endpoint == self.bemuch.endpoint:
            if phone in self.phonebook:
                item = ObjectDict(self.phonebook[phone])
                item.phone = phone
                return item
            else:
                raise tornado.web.HTTPError(404)
        else:
            item = yield self.bemuch.call(endpoint, 'read', phone)
            return item

    @coroutine
    def update(self, phone, item):
        endpoint = self.selector(phone)
        if endpoint == self.bemuch.endpoint:
            if phone in self.phonebook:
                self.phonebook[phone].update(item)
                logging.info("Created on {} item: {}"
                             "".format(endpoint, (phone,
                                                  item.first_name,
                                                  item.last_name)))
            else:
                self.phonebook[phone] = item
                logging.info("Updated on {} item: {}"
                             "".format(endpoint, (phone,
                                                  item.first_name,
                                                  item.last_name)))
        else:
            yield self.bemuch.call(endpoint, 'update', phone, item)

    @coroutine
    def delete(self, phone):
        endpoint = self.selector(phone)
        if endpoint == self.bemuch.endpoint:
            if phone in self.phonebook:
                item = self.phonebook.pop(phone)
                logging.info("Deleted from {} item: {}"
                             "".format(endpoint, (phone,
                                                  item.first_name,
                                                  item.last_name)))
            else:
                raise tornado.web.HTTPError(404)
        else:
            yield self.bemuch.call(endpoint, 'delete', phone)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    shard_cnt = 4  # may up to 64
    for idx in range(0, shard_cnt):
        endpoint = "#{}".format(idx)
        bemuch.startApp(endpoint, CrudApplication, 8000+idx,
                        lambda key: "#{}".format(int(key) % shard_cnt))
    bemuch.start()
