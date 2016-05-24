import asyncio
import os

from pymongo import MongoClient

from .agent import OnlineAgent, ScriptAgent
from jsonprotocol import JsonProtocol


class SupervisorServer(JsonProtocol):
    def __init__(self, supervisor):
        self.supervisor = supervisor

    def connection_made(self, transport):
        super().connection_made(transport)

    def received(self, obj):
        try:
            identifier = str(obj['identifier'])
            token = str(obj['token'])
            action = str(obj['action'])
            payload = str(obj['payload'])
        except KeyError:
            print("request is missing one or more fields: {token, identifier, action, payload}")
            self.send({'error': 'authentication failed, request is missing token or identifier'})
            return

        if identifier == 'sensor':
            ans = self.supervisor.sensor_request(token, action, payload)
            self.send(ans)
        else:
            ans = self.supervisor.analyzer_request(identifier, token, action, payload)
            self.send(ans)


class SupervisorClient(JsonProtocol):
    """
    Simple request-answer based client.
    """
    def __init__(self, host, port, identifier, token):
        self.identifier = identifier
        self.token = token

        # get fresh and empty event loop
        self.loop = asyncio.new_event_loop()

        # connect to server
        coro = self.loop.create_connection(lambda: self, host, port)
        self.loop.run_until_complete(coro)

    def request(self, action: str, payload: dict = None):
        msg = {
            'identifier': self.identifier,
            'token': self.token,
            'action': action,
            'payload': payload
        }

        # send request
        self.send(msg)

        # wait until self.received() is called and _current contains the response
        # NOTE: obviously this pattern will fail if the server doesn't send exactly the same number of
        # answers than the number of received requests.
        self.loop.run_forever()
        return self._current

    def received(self, obj):
        self._current = obj
        self.loop.stop()


class Supervisor:
    """

    """
    def __init__(self, loop, mongo, sensor_token, supervisor_host='localhost', supervisor_port=33424):
        """
        :param mongo: MongoDB client connection with rights to create users on the analyzer database
        """
        self.loop = loop
        self.mongo = mongo

        self.supervisor_host = supervisor_host
        self.supervisor_port = supervisor_port

        self.sensor_token = sensor_token
        self.agents = {}

        # todo delete users and collections

        self.next_identifier = 0

    def analyzer_request(self, identifier, token, action, payload):
        try:
            agent = self.agents[identifier]
        except KeyError:
            print("no analyzer with this identifier")
            return {'error': 'authentication failed, analyzer not on record with this identifier'}

        if agent.token == token:
            return agent.handle_request(action, payload)
        else:
            return {'error': 'authentication failed, token incorrect'}

    def sensor_request(self, token, action, payload):
        if token != self.sensor_token:
            return {'error': 'authentication failed, token incorrect'}

        if action == 'orders':
            pass

    def _create_identifier(self):
        # TODO: more intelligent?
        identifier = self.next_identifier
        self.next_identifier += 1
        return "an{}".format(identifier)

    def _create_bootstrap(self):
        identifier = self._create_identifier()
        token = os.urandom(16).hex()
        return {'token': token, 'identifier': identifier, 'host': self.supervisor_host, 'port': self.supervisor_port}

    async def create_online_agent(self):
        print("creating online supervisor")
        bootstrap = self._create_bootstrap()
        agent = OnlineAgent(bootstrap['identifier'], bootstrap['token'], self.mongo)
        self.agents[bootstrap['identifier']] = agent

        await agent.startup()

        return bootstrap, agent

    async def create_script_agent(self, cmdline):
        """
        :param cmdline: Command line to run
        """
        print("creating script supervisor")
        bootstrap = self._create_bootstrap()
        agent = ScriptAgent(bootstrap, self.mongo, cmdline)
        self.agents[bootstrap['identifier']] = agent

        return agent

    def start(self):
        print("starting server...")
        server_coro = self.loop.create_server(lambda: SupervisorServer(self), host=self.supervisor_host, port=self.supervisor_port)
        self.server = self.loop.run_until_complete(server_coro)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")

    cur = Supervisor(loop, mongo, 'abcdefg')

    # create online supervisor and print account details
    bootstrap, agent = loop.run_until_complete(cur.create_online_agent())
    print(bootstrap)

    cur.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(agent.teardown())