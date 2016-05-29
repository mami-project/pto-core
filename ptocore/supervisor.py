import asyncio
import json
import traceback
from typing import Set

import os
from functools import partial
from pymongo import MongoClient
from pymongo.collection import Collection

from .agent import OnlineAgent, ScriptAgent
from .analyzerstate import AnalyzerState
from .jsonprotocol import JsonProtocol
from .mongoutils import AutoIncrementFactory


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
            self.send({'error': 'request is missing one or more fields: {token, identifier, action, payload}'})
            return

        ans = self.supervisor.analyzer_request(identifier, token, action, payload)
        self.send(ans)


class Supervisor:
    def __init__(self, mongo: MongoClient, analyzers_coll: Collection, id_coll: Collection, loop=None, host='localhost', port=33424):
        self.loop = loop or asyncio.get_event_loop()

        id_factory = AutoIncrementFactory(id_coll)
        self._action_id_creator = id_factory.get_incrementor('action_id')
        self._online_id_creator = id_factory.get_incrementor('online_id')

        self.host = host
        self.port = port

        self.mongo = mongo
        self.analyzers_state = AnalyzerState(analyzers_coll)

        self.agents = {}

        self.server = None

        # todo delete users and collections

        server_coro = self.loop.create_server(lambda: SupervisorServer(self), host=self.host, port=self.port)
        self.server = self.loop.run_until_complete(server_coro)

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

    def shutdown_online_agent(self, agent):
        agent.teardown()
        del self.agents[agent.identifier]

    def create_online_agent(self):
        print("creating online supervisor")
        online_id = self._online_id_creator()
        token = os.urandom(16).hex()

        agent = OnlineAgent(online_id, token, self.mongo)

        self.agents[agent.identifier] = agent

        credentials = { 'identifier': agent.identifier, 'token': token, 'host': self.host, 'port': self.port }

        return credentials, agent

    def script_agent_done(self, agent: ScriptAgent, fut: asyncio.Future):
        print("script agent done")
        agent.teardown()
        del self.agents[agent.identifier]

        try:
            # raise exceptions that happened in the future
            fut.result()
        except Exception as e:
            # an error happened
            traceback.print_exc()

            # set state accordingly
            self.analyzers_state.transition_to_error(agent.analyzer_id, traceback.format_exc())
        else:
            # everything went well, so give to validator
            self.analyzers_state.transition_to_executed(agent.analyzer_id, agent.result_max_action_id, agent.result_timespans)

    def check_for_work(self):
        planned = self.analyzers_state.planned_analyzers()
        print("check for work")
        for analyzer in planned:
            print("planned", analyzer)

            # create agent
            action_id = self._action_id_creator()
            token = os.urandom(16).hex()

            agent = ScriptAgent(analyzer['_id'], action_id, token, self.host, self.port,
                                analyzer['command_line'], analyzer['working_dir'],
                                analyzer['input_formats'], analyzer['input_types'], analyzer['output_types'], self.mongo)

            self.agents[agent.identifier] = agent

            # change analyzer state
            self.analyzers_state.transition_to_executing(agent.analyzer_id, agent.identifier)

            # schedule for execution
            task = asyncio.ensure_future(agent.execute())
            task.add_done_callback(partial(self.script_agent_done, agent))
            print("script agent started")

    async def run(self):
        while True:
            await asyncio.sleep(4)
            self.check_for_work()


def main():
    loop = asyncio.get_event_loop()
    mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")

    sup = Supervisor(mongo, mongo.analysis.analyzers, mongo.analysis.idfactory, loop)

    # create online supervisor and print account details
    credentials, agent = sup.create_online_agent()
    print(json.dumps(credentials))

    asyncio.ensure_future(sup.run())
    loop.run_forever()

if __name__ == "__main__":
    main()