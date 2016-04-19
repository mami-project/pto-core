import asyncio
from jsonprotocol import JsonProtocol
from pymongo import MongoClient

from .supervisor import SupervisorOnline, SupervisorScript
import os

class CuratorAnalyzerServer(JsonProtocol):
    def __init__(self, curator):
        self.curator = curator

    def connection_made(self, transport):
        super().connection_made(transport)

    def received(self, obj):
        # determine corresponding analysis object and pass on the request
        try:
            identifier = str(obj['identifier'])
            token = str(obj['token'])
        except KeyError:
            print("request is missing token or identifier")
            self.send({'error': 'authentication failed, request is missing token or identifier'})
            return

        try:
            analyzer = self.curator.get_analyzer(identifier)
        except KeyError:
            print("no analyzer with this identifier")
            self.send({'error': 'authentication failed, analyzer not on record with this identifier'})
            return

        if analyzer.token == token:
            ans = analyzer.handle_request(obj)
            self.send(ans)
        else:
            self.send({'error': 'authentication failed, token incorrect'})

class Curator:
    """
    The curator is the overall manager of the analysis engine. Its duties are:
     - grant and revoke access to the database
     - control the sensor and validator processes
     - execute analyzers
     - manages overall workflow and ensures that generated observations are valid
     - is controlled via websocket interface

     The sensor, validator and analyzers are not run together in the same python interpreter because all of these will
     dynamically load and unload code. This would subvert access control.

     Also the goal is to concentrate all state information in the curator process with the goal that all other components
     can be written state-less.
    """
    def __init__(self, loop, mongo, analyzer_host='localhost', analyzer_port=33424):
        """
        Upon creation, clean up the environment:
        - delete all users with username starting with 'analyzer_'
        - delete all (temporary) collections in the analysis database.

        :param mongo: MongoDB client connection with rights to create users on the analyzer database
        """
        self.loop = loop
        self.mongo = mongo

        self.analyzer_host = analyzer_host
        self.analyzer_port = analyzer_port

        self.sensor = None
        self.validator = None
        self.supervisors = {}

        # todo delete users and collections

        self.next_identifier = 0

    def get_analyzer(self, identifier):
        return self.supervisors[identifier]

    def _create_identifier(self):
        # TODO: more intelligent?
        identifier = self.next_identifier
        self.next_identifier += 1
        return "an{}".format(identifier)

    def _create_bootstrap(self):
        identifier = self._create_identifier()
        token = os.urandom(16).hex()
        return {'token': token, 'identifier': identifier, 'host': self.analyzer_host, 'port': self.analyzer_port}

    async def create_online_supervisor(self):
        print("creating online supervisor")
        bootstrap = self._create_bootstrap()
        supervisor = SupervisorOnline(bootstrap['identifier'], bootstrap['token'], self.mongo)
        self.supervisors[bootstrap['identifier']] = supervisor

        await supervisor.startup()

        return bootstrap, supervisor

    async def create_script_supervisor(self, cmdline):
        """
        :param cmdline: Command line to run
        """
        print("creating script supervisor")
        # TODO: provide params over CuratorAnalysisServer in addition to command line.
        # (because JSON is more fun than cmdline arguments)

        bootstrap = self._create_bootstrap()
        supervisor = SupervisorScript(bootstrap, self.mongo, cmdline)
        self.supervisors[bootstrap['identifier']] = supervisor

        return supervisor

    def start(self):
        print("starting servers...")
        # Start communication server for analyzers.
        analyzer_server_coro = self.loop.create_server(lambda: CuratorAnalyzerServer(self), host='localhost', port=33424)
        self.analyzer_server = self.loop.run_until_complete(analyzer_server_coro)

        # Start websocket listener for the web control panel.
        # control_coro =
        # self.control_server = self.loop.run_until(control_coro)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")

    cur = Curator(loop, mongo)

    # create online supervisor and print account details
    bootstrap, supervisor = loop.run_until_complete(cur.create_online_supervisor())
    print(bootstrap)

    cur.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(supervisor.teardown())