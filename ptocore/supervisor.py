import asyncio
import json
import traceback
from typing import Set
import argparse

import os
from functools import partial
from pymongo.database import Database
from pymongo.collection import Collection
import dpath.util

from .agent import OnlineAgent, ModuleAgent
from .analyzerstate import AnalyzerState
from .jsonprotocol import JsonProtocol
from .mongoutils import AutoIncrementFactory
from .coreconfig import CoreConfig


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
            payload = obj['payload']
        except KeyError:
            print("request is missing one or more fields: {token, identifier, action, payload}")
            self.send({'error': 'request is missing one or more fields: {token, identifier, action, payload}'})
            return

        ans = self.supervisor.analyzer_request(identifier, token, action, payload)
        self.send(ans)


class Supervisor:
    def __init__(self, core_config: CoreConfig, loop=None):
        self.loop = loop or asyncio.get_event_loop()

        self.core_config = core_config

        # delete all users starting with `module-` or `online-` in their name
        self.delete_temp_users()

        # the supervisor is the only component generating agent_ids, therefore create_if_missing=True is not a problem.
        idfactory = AutoIncrementFactory(self.core_config.idfactory_coll)
        self._agent_id_creator = idfactory.get_incrementor('agent_id', create_if_missing=True)

        self.analyzer_state = AnalyzerState('supervisor', self.core_config.analyzers_coll)

        self.agents = {}

        self.server = None

        server_coro = self.loop.create_server(lambda: SupervisorServer(self),
                                              host='localhost',
                                              port=self.core_config.supervisor_port)
        self.server = self.loop.run_until_complete(server_coro)

    def delete_temp_users(self):
        # delete existing users and roles attached to this supervisor
        temp_db = self.core_config.temporary_db

        userdicts = temp_db.command({ "usersInfo": 1})
        userdbnames = dpath.util.values(userdicts, 'users/*/_id')

        # userdbnames is a list of <database>.<username>
        # now split <database>. away.
        usernames = [userdbname.split('.', 1)[1] for userdbname in userdbnames]

        # only care about `online-` and `module-` users
        usernames = [un for un in usernames if un.startswith('online_') or un.startswith('module_')]

        for username in usernames:
            print("dropping", username)
            temp_db.remove_user(username)
            temp_db.command("dropRole", username)

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

        # create agent
        identifier = 'online_'+str(self._agent_id_creator())
        token = os.urandom(16).hex()

        agent = OnlineAgent(identifier, token, self.core_config)

        self.agents[agent.identifier] = agent

        credentials = { 'identifier': agent.identifier, 'token': token,
                        'host': 'localhost', 'port': self.core_config.supervisor_port }

        return credentials, agent

    def script_agent_done(self, agent: ModuleAgent, fut: asyncio.Future):
        print("module agent done")
        agent.teardown()
        del self.agents[agent.identifier]

        try:
            # raise exceptions that happened in the future
            fut.result()
        except Exception as e:
            # an error happened
            traceback.print_exc()

            # set state accordingly
            self.analyzer_state.transition_to_error(agent.analyzer_id,
                                                     "error when exeucting analyzer module:\n" + traceback.format_exc())
        else:
            # everything went well, so give to validator
            transition_args = {'execution_result': {
                'temporary_coll': agent.identifier,
                'max_action_id': agent.result_max_action_id,
                'timespans': agent.result_timespans
            }}

            self.analyzer_state.transition(agent.analyzer_id, 'executing', 'executed', transition_args)

    def check_for_work(self):
        planned = self.analyzer_state.planned_analyzers()
        print("supervisor: check for work")
        for analyzer in planned:
            # check for wish
            if self.analyzer_state.check_wish(analyzer, 'cancel'):
                print("supervisor: cancelled {} upon request".format(analyzer['_id']))
                continue

            print("executing", analyzer['_id'])

            # create agent
            identifier = 'module_'+str(self._agent_id_creator())
            token = os.urandom(16).hex()

            agent = ModuleAgent(analyzer['_id'], identifier, token, self.core_config,
                                analyzer['input_formats'], analyzer['input_types'], analyzer['output_types'],
                                analyzer['command_line'], analyzer['working_dir'], analyzer['rebuild_all'],
                                self.core_config.supervisor_ensure_clean_repo)

            self.agents[agent.identifier] = agent

            # change analyzer state
            self.analyzer_state.transition(agent.analyzer_id, 'planned', 'executing')

            # schedule for execution
            task = asyncio.ensure_future(agent.execute())
            task.add_done_callback(partial(self.script_agent_done, agent))
            print("module agent started")

    async def run(self):
        while True:
            self.check_for_work()
            await asyncio.sleep(4)


def main():
    desc = 'Manage execution of analyzer modules.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('CONFIG_FILES', type=argparse.FileType('rt'), nargs='*')
    args = parser.parse_args()

    cc = CoreConfig('supervisor', args.CONFIG_FILES)

    loop = asyncio.get_event_loop()

    sup = Supervisor(cc, loop)

    # create online supervisor and print account details
    credentials, agent = sup.create_online_agent()
    print(json.dumps(credentials))
    print("export PTO_CREDENTIALS=\"{}\"".format(json.dumps(credentials).replace('"', '\\"')))

    asyncio.ensure_future(sup.run())
    loop.run_forever()

if __name__ == "__main__":
    main()