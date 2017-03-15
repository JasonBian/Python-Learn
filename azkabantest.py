
from getpass import getuser

from azkaban import Job, Project

PROJECT = Project('sample')

PROJECT.properties = {
    'user.to.proxy': getuser(),
}

JOBS = {
    'first': Job({'type': 'command', 'command': 'echo "Hello"'}),
    'second': Job({'type': 'command', 'command': 'echo "world'}),
    'third': Job({'type': 'noop', 'dependencies': 'first,second'}),
}

for name, job in JOBS.items():
    PROJECT.add_job(name, job)
