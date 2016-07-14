import os
import re
import subprocess
from typing import Sequence
import json
import shutil


class RepositoryError(Exception):
    pass


class GitError(RepositoryError):
    pass


class NameNotAllowedError(RepositoryError):
    pass


_expr_allowed_name = re.compile(r'[a-zA-z0-9_\-]+')

# TODO move to unittests
assert(_expr_allowed_name.fullmatch('test_123-4'))
assert(not _expr_allowed_name.fullmatch('test.4'))
assert(not _expr_allowed_name.fullmatch('test../abc'))
assert(not _expr_allowed_name.fullmatch('file://'))


def git_cmd(repo_path: str, args: Sequence[str]) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=repo_path)
    except subprocess.CalledProcessError as e:
        raise GitError("git reported an error: " + e.stdout.decode('utf-8')) from e


def clean_repository(repo_path: str):
    git_cmd(repo_path, ['git', 'reset', '-q', '--hard'])
    git_cmd(repo_path, ['git', 'clean', '-q', '-d', '-x', '-f'])


def procure_repository(base_path: str, analyzer_id: str, repo_url: str, repo_commit: str):
    if len(analyzer_id) == 0:
        raise NameNotAllowedError("analyzer_id has length zero.")

    if _expr_allowed_name.fullmatch(analyzer_id) is None:
        raise NameNotAllowedError()

    repo_path = os.path.join(base_path, analyzer_id)

    if os.path.exists(repo_path):
        # delete analyzer repository
        shutil.rmtree(repo_path)

    git_cmd(base_path, ['git', 'clone', repo_url, repo_path])

    clean_repository(repo_path)
    git_cmd(repo_path, ['git', 'fetch', '-q'])
    git_cmd(repo_path, ['git', 'checkout', repo_commit])

    config_fn = os.path.join(repo_path, 'ptocore.json')

    with open(config_fn) as fp:
        return json.load(fp)

def get_repository_url(repo_path: str):
    ans = git_cmd(repo_path, ['git', 'config', '--get', 'remote.origin.url'])
    return ans.stdout.decode().strip()

def get_repository_commit(repo_path: str):
    ans = git_cmd(repo_path, ['git', 'rev-parse', 'HEAD'])
    return ans.stdout.decode().strip()

def get_repository_url_commit(repo_path: str):
    return get_repository_url(repo_path), get_repository_commit(repo_path)
