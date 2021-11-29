import glob
import os

from pony.orm import core
from pony.utils import throw

from pony.orm.migrations.migrate import Migration


class MigrationGraph(object):
    def __init__(self, migrations_dir):
        # self.db = db
        self.dir = migrations_dir
        self.files = sorted([os.path.basename(file) for file in glob.glob(os.path.join(self.dir, '*.py'))])
        self.squashed = {}
        self.migrations = {}
        for file in self.files:
            mig = Migration.load(self.dir, file)
            if mig.based_on:
                for base in mig.based_on:
                    squashed_mig = self.migrations.pop(base)
                    self.squashed[base] = squashed_mig
            self.migrations[file[:-3]] = mig
        # link parents
        for name, migration in self.migrations.items():
            for dep in migration.dependencies:
                if dep not in self.migrations:
                    throw(core.MigrationError, "Dependency `%s` for migration `%s` not found" % (dep, name))
                migration.parents.append(self.migrations[dep])

    def make_dependencies(self):
        # print(self.migrations)
        all_migrations = set(self.migrations.keys())
        parent_migrations = set()
        for migration in self.migrations.values():
            for parent in migration.dependencies:
                parent_migrations.add(parent)

        return list(all_migrations - parent_migrations)

    def make_route(self):
        # TODO LINEAR WAY ONLY IMPLEMENTED
        start = self.make_dependencies()
        # assert len(start) == 1, start
        if not start:
            return []
        first = self.migrations[start[0]]
        route = [first]
        while route[-1].parents:
            parents = route[-1].parents
            assert len(parents) == 1, parents
            parent = parents[0]
            route.append(parent)
        return list(reversed(route))
