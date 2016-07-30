import asyncio
import json

class JsonProtocol(asyncio.Protocol):
    """
    Implements asyncio's Protocol pattern for transferring data in the form of line-separated json-encoded
    messages.
    """
    MAX_BUFSIZE = 1024*1024*20

    def connection_made(self, transport):
        self.transport = transport
        self.__buffer = ''

    def data_received(self, data):
        decoded = data.decode()

        if len(decoded) + len(self.__buffer) > JsonProtocol.MAX_BUFSIZE:
            print("buffer too big")
            self.__buffer = ''

        self.__buffer += decoded

        if '\n' in decoded:
            message_str, self.__buffer = self.__buffer.split('\n', 1)

            try:
                obj = json.loads(message_str)
            except json.JSONDecodeError:
                print("error decoding message")
                # TODO: log
            else:
                self.received(obj)

    def send(self, obj):
        s = json.dumps(obj) + "\n"
        return self.transport.write(s.encode())

    def received(self, obj):
        raise NotImplementedError()