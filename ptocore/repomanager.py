import os
import re
import subprocess


class RepoError(Exception):
    pass


class NameNotAllowedError(RepoError):
    pass


_expr_allowed_name = re.compile(r'[a-zA-z0-9_\-]+')

# TODO move to unittests
assert(_expr_allowed_name.fullmatch('test_123-4'))
assert(not _expr_allowed_name.fullmatch('test.4'))
assert(not _expr_allowed_name.fullmatch('test../abc'))
assert(not _expr_allowed_name.fullmatch('file://'))


def git_cmd(repo_path, args):
    subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=repo_path)


def procure_repository(base_path, analyzer_id, repo_url, repo_commit):
    if _expr_allowed_name.fullmatch(analyzer_id) is None:
        raise NameNotAllowedError()

    repo_path = os.path.join(base_path, analyzer_id)

    if not os.path.exists(repo_path):
        # directory doesn't exist, clone it
        git_cmd(base_path, ['git', 'clone', repo_url])
    else:
        # directory does exist, set remote url
        git_cmd(repo_path, ['git', 'remote', 'set-url', 'origin', repo_url])

    git_cmd(repo_path, ['git', 'reset', '-q', '--hard'])
    git_cmd(repo_path, ['git', 'clean', '-q', '-d', '-x', '-f'])
    git_cmd(repo_path, ['git', 'fetch', '-q'])
    git_cmd(repo_path, ['git', 'checkout', repo_commit])
