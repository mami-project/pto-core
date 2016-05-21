import asyncio
from jsonprotocol import JsonProtocol
from pymongo import MongoClient

from .agent import OnlineAgent, ScriptAgent
import os

class SupervisorServer(JsonProtocol):
    def __init__(self, curator):
        self.agent = curator

    def connection_made(self, transport):
        super().connection_made(transport)

    def received(self, obj):
        # determine corresponding agent object and pass on the request to it
        try:
            identifier = str(obj['identifier'])
            token = str(obj['token'])
        except KeyError:
            print("request is missing token or identifier")
            self.send({'error': 'authentication failed, request is missing token or identifier'})
            return

        try:
            agent = self.agent.get_agent(identifier)
        except KeyError:
            print("no analyzer with this identifier")
            self.send({'error': 'authentication failed, analyzer not on record with this identifier'})
            return

        if agent.token == token:
            ans = agent.handle_request(obj)
            self.send(ans)
        else:
            self.send({'error': 'authentication failed, token incorrect'})

class Supervisor:
    """

    """
    def __init__(self, loop, mongo, analyzer_host='localhost', analyzer_port=33424):
        """
        :param mongo: MongoDB client connection with rights to create users on the analyzer database
        """
        self.loop = loop
        self.mongo = mongo

        self.analyzer_host = analyzer_host
        self.analyzer_port = analyzer_port

        self.sensor = None
        self.validator = None
        self.agents = {}

        # todo delete users and collections

        self.next_identifier = 0

    def get_agent(self, identifier):
        return self.agents[identifier]

    def _create_identifier(self):
        # TODO: more intelligent?
        identifier = self.next_identifier
        self.next_identifier += 1
        return "an{}".format(identifier)

    def _create_bootstrap(self):
        identifier = self._create_identifier()
        token = os.urandom(16).hex()
        return {'token': token, 'identifier': identifier, 'host': self.analyzer_host, 'port': self.analyzer_port}

    async def create_online_agent(self):
        print("creating online supervisor")
        bootstrap = self._create_bootstrap()
        supervisor = OnlineAgent(bootstrap['identifier'], bootstrap['token'], self.mongo)
        self.agents[bootstrap['identifier']] = supervisor

        await supervisor.startup()

        return bootstrap, supervisor

    async def create_script_agent(self, cmdline):
        """
        :param cmdline: Command line to run
        """
        print("creating script supervisor")
        # TODO: provide params over CuratorAnalysisServer in addition to command line.
        # (because JSON is more fun than cmdline arguments)

        bootstrap = self._create_bootstrap()
        supervisor = ScriptAgent(bootstrap, self.mongo, cmdline)
        self.agents[bootstrap['identifier']] = supervisor

        return supervisor

    def start(self):
        print("starting servers...")
        # Start communication server for analyzers.
        analyzer_server_coro = self.loop.create_server(lambda: SupervisorServer(self), host='localhost', port=33424)
        self.analyzer_server = self.loop.run_until_complete(analyzer_server_coro)

        # Start websocket listener for the web control panel.
        # control_coro =
        # self.control_server = self.loop.run_until(control_coro)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")

    cur = Supervisor(loop, mongo)

    # create online supervisor and print account details
    bootstrap, agent = loop.run_until_complete(cur.create_online_agent())
    print(bootstrap)

    cur.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(agent.teardown())