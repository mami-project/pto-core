from jsonprotocol import JsonProtocol
import asyncio

class JsonClient(JsonProtocol):
    def __init__(self, host, port):

        self.loop = asyncio.get_event_loop()
        coro = self.loop.create_connection(lambda: self, host, port)
        self.loop.run_until_complete(coro)

    def recv(self):
        self.loop.run_forever()
        return self._current

    def received(self, obj):
        self._current = obj
        self.loop.stop()
