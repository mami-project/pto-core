import pygit2
import os
import re
import subprocess

class RepoError(Exception):
    pass

class RepoLockedError(RepoError):
    pass

class RepoNotKnownError(Exception):
    pass

class RepoNameNotAllowedError(RepoError):
    pass

class RepoDirectoryExists(RepoError):
    pass

class RepoGitError(RepoError):
    pass


class Repo:
    def __init__(self, path, git, locked):
        self.path = path
        self.git = git
        self.locked = locked

# TODO move to unittests
_expr_allowed_name = re.compile(r'[a-zA-z0-9_\-]+')
assert(_expr_allowed_name.fullmatch('test_123-4'))
assert(not _expr_allowed_name.fullmatch('test.4'))
assert(not _expr_allowed_name.fullmatch('test../abc'))
assert(not _expr_allowed_name.fullmatch('file://'))

class RepoManager:
    """
    this class is not thread-safe
    """
    def __init__(self, path):
        """

        :param path:
        :return:
        """
        self.path = path
        self.repositories = {}

        # discover repositories
        for entry in os.scandir(self.path):
            if entry.is_dir():
                try:
                    git_path = pygit2.discover_repository(entry.path)
                except KeyError:
                    # given directory is not a repository
                    # TODO: issue warning, repository directory should ONLY contain repositories
                    continue

                self.repositories[entry.name] = Repo(path=entry.path, git=pygit2.Repository(git_path), locked=False)

    def register(self, analyzer_name, repo_url):
        """
        :param analyzer_name:
        :param repo_url:
        :return:
        :raise: RepoLockedError if repository is already registered and is locked. The remote url cannot be changed when repository is in use.
        :raise: GitError if anything git related fails.
        """

        if _expr_allowed_name.fullmatch(analyzer_name) is None:
            raise RepoNameNotAllowedError()

        if analyzer_name not in self.repositories:
            # repository does not exist, clone.

            repo_path = os.path.join(self.path, analyzer_name)
            if os.path.exists(repo_path):
                raise RepoDirectoryExists("I will not clone into existing directory.")

            git = pygit2.clone_repository(repo_url, repo_path)
            repo = Repo(path=repo_path, git=git, locked=False)

            self.repositories[analyzer_name] = repo
        else:
            # repository does exist, set-url.
            repo = self.repositories[analyzer_name]

            if repo.locked:
                raise RepoLockedError()

            current_url = repo.git.remotes['origin'].url

            if current_url != repo_url:
                repo.git.remotes.set_url('origin', repo_url)

    def checkout_and_lock(self, analyzer_name, commit):
        """
        Checkout a specific version of the analyzer and lock it such that it cannot be changed.

        :param repo_url:
        :return:
        """

        # TODO: check commit for a-zA-Z0-9

        # TODO: only run once at a time

        # TODO: check if we already have repository, if no call register

        repo = self.repositories[analyzer_name]

        if repo.locked:
            raise RepoLockedError()

        repo.locked = True


        # TODO check if commit is available or i need to fetch it

        # TODO add errors to log
        cmds = [
            ['git', 'reset', '-q', '--hard'],
            ['git', 'clean', '-q', '-d', '-x', '-f'],
            ['git', 'fetch', '-q'],
            ['git', 'checkout', commit]
        ]

        for cmd in cmds:
            proc = subprocess.Popen(cmd, cwd=repo.path)

            (stderr, stdout) = proc.communicate()
            print(stderr)
            print(stdout)
            print(proc.returncode)

            if proc.returncode != 0:
                raise RepoGitError()

    def release(self, analyzer_name):
        repo = self.repositories[analyzer_name]
        repo.locked = False
