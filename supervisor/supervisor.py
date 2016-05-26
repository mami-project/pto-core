import asyncio
import traceback

import os
from typing import Sequence
from functools import partial

from pymongo import MongoClient

from .agent import OnlineAgent, ScriptAgent
from jsonprotocol import JsonProtocol
from analyzerstate import AnalyzerState
from mongoutils import AutoIncrementFactory

from collections import namedtuple

Credentials = namedtuple('Credentials', ['identifier', 'token', 'host', 'port'])

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
    def __init__(self, credentials: Credentials):
        self.identifier = credentials.identifier
        self.token = credentials.token

        # get fresh and empty event loop
        self.loop = asyncio.new_event_loop()

        # connect to server
        coro = self.loop.create_connection(lambda: self, credentials.host, credentials.port)
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
    def __init__(self, loop: asyncio.BaseEventLoop, mongo: MongoClient, sensor_token, supervisor_host='localhost', supervisor_port=33424):
        """
        :param mongo: MongoDB client connection with rights to create users on the analyzer database
        """
        self.loop = loop
        self.mongo = mongo

        self.check_interval = 10

        factory = AutoIncrementFactory(mongo.analysis.idfactory)

        # maybe change that to action_id
        # maybe seperate id for script (action_id) and online (something else)
        self._action_id_creator = factory.get_incrementor('action_id')
        self._online_id_creator = factory.get_incrementor('online_id')

        self.supervisor_host = supervisor_host
        self.supervisor_port = supervisor_port

        self.analyzers_state = AnalyzerState(mongo.analysis.analyzers)

        self.sensor_token = sensor_token
        self.agents = {}

        # todo delete users and collections

    def start(self):
        print("starting server...")
        server_coro = self.loop.create_server(lambda: SupervisorServer(self), host=self.supervisor_host, port=self.supervisor_port)
        self.server = self.loop.run_until_complete(server_coro)

        self.loop.call_later(self.check_interval, self.check_for_work)


    def analyzer_request(self, identifier, token, action, payload):
        try:
            agent = self.agents[identifier]
        except KeyError:
            print("no analyzer with this identifier")
            return {'error': 'authentication failed, analyzer not on record with this identifier'}

        if agent.token == token:
            return agent._handle_request(action, payload)
        else:
            return {'error': 'authentication failed, token incorrect'}

    def sensor_request(self, token, action, payload):
        if token != self.sensor_token:
            return {'error': 'authentication failed, token incorrect'}

        if action == 'orders':
            pass

    def shutdown_online_agent(self, agent):
        agent.teardown()
        del self.agents[agent.identifier]

    def create_online_agent(self):
        print("creating online supervisor")
        credentials = Credentials(self._online_id_creator(), os.urandom(16).hex(), self.supervisor_host, self.supervisor_port)

        agent = OnlineAgent(credentials.identifier, credentials.token, self.mongo)

        self.agents[credentials.identifier] = agent

        return credentials, agent

    def create_script_agent(self, analyzer_id, cmdline: Sequence[str]) -> ScriptAgent:
        """
        :param cmdline: Command line to run
        """
        print("creating script supervisor")
        credentials = Credentials(self._action_id_creator(), os.urandom(16).hex(), self.supervisor_host, self.supervisor_port)

        agent = ScriptAgent(analyzer_id, credentials, cmdline, self.mongo)
        self.agents[agent.identifier] = agent

        return agent

    def script_agent_done(self, agent, fut: asyncio.Future):
        print("script agent done")
        agent.teardown()
        del self.agents[agent.identifier]

        try:
            # raise exceptions happening in future
            fut.result()
        except Exception as e:
            # an error happened
            traceback.print_exc()

            # set state accordingly
            self.analyzers_state.transition_to_error(agent.analyzer_id, traceback.format_exc())
        else:
            # everything went well, so give to validator
            self.analyzers_state.transition_to_executed(agent.analyzer_id)


    def check_for_work(self):
        print("check for work")
        planned = self.analyzers_state.planned_analyzers()
        for analyzer in planned:
            agent = self.create_script_agent(analyzer['_id'], analyzer['cmdline'])

            self.analyzers_state.transition_to_executing(agent.analyzer_id, agent.identifier)

            # schedule for execution
            task = asyncio.ensure_future(agent.execute())
            task.add_done_callback(partial(self.script_agent_done, agent))
            print("script agent started")

        self.loop.call_later(self.check_interval, self.check_for_work)

def main():
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

if __name__ == "__main__":
    main()