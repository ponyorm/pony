from __future__ import unicode_literals
from pony.py23compat import ExitStack, suppress, PY2

import re, os, os.path, sys, warnings
from collections import deque, OrderedDict
from functools import total_ordering
from datetime import datetime
from textwrap import dedent
from glob import glob

import pony
from pony import orm
from pony.utils import throw, reraise

from . import get_cmd_exitstack, get_migration_dir
from .exceptions import MigrationFileCorrupted, CircularDependencyError, NodeNotFoundError, \
    UnmergedMigrationsDetected
from .operations import CustomOp
from .serializer import serializer_factory
from .utils import run_path
from .writer import MigrationWriter, MIGRATION_TEMPLATE


def make_migration_entity(db):
    db.migration_in_progress = True
    class Migration(db.Entity):
        name = orm.Required(str)
        applied = orm.Required(datetime)


def parse_number(name):
    """
    Given a migration name, tries to extract a number from the
    beginning of it. If no number found, returns None.
    """
    match = re.match(r'^\d+', name)
    if match:
        return int(match.group())
    return None


def run_migration_file(name):
    path = os.path.join(
       get_migration_dir(), '{}.py'.format(name)
    )
    return run_path(path)


def upgrade_db(db, module):
    operations = module.get('operations', [])
    define_entities = module.get('define_entities')
    if define_entities:
        define_entities(db)
        db.generate_schema()
    for op in operations:
        op.apply(db)


def reconstruct_db(db):
    args, kwargs = db._constructor_args
    return db.__class__(*args, **kwargs)


class Migration(object):
    def __init__(self, name=None, graph=None):
        self.name = name
        self.graph = graph
        self.operations = []
        self.dependencies = []

    def __repr__(self):
        return 'Migration(%r)' % self.name

    @classmethod
    def _generate_name(cls, graph, name=None):
        highest_number = 0
        for leaf in graph.leaf_nodes():
            num = parse_number(leaf)
            if num is not None and num > highest_number:
                highest_number = num
        highest_number += 1
        if highest_number == 1:
            return "0001_initial"
        if name is None:
            name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return "%04i_%s" % (highest_number, name)


    @classmethod
    def make(cls, db, empty=False, custom=False, filename=None):
        get_cmd_exitstack().callback(db.disconnect)
        graph = MigrationGraph()
        leaves = graph.leaf_nodes()
        if len(leaves) > 1:
            from pony.migrate import command
            command.merge(db, graph, leaves)
            return
        migrations = get_migration_dir()
        if not os.path.exists(migrations):
            os.mkdir(migrations)
        if not leaves:
            generated = MigrationWriter(leaves, None, db).as_string()
            name = '0001_initial'
            p = os.path.join(migrations, '{}.py'.format(name))
            with open(p, 'w') as f:
                f.write(generated)
            print('Written: %s' % os.path.relpath(p))
            return

        [leaf] = leaves
        plan = graph.forwards_plan(leaf)
        initial = plan[0]
        namespace = run_migration_file(initial)
        define_entities = namespace['define_entities']
        prev_db = reconstruct_db(db)
        get_cmd_exitstack().callback(prev_db.disconnect)
        define_entities(prev_db)
        prev_db.generate_schema()
        for name in plan[1:]:
            mod = run_migration_file(name)
            upgrade_db(prev_db, mod)

        db.generate_schema()
        prev_db.generate_schema()

        name = cls._generate_name(graph, filename)
        if empty:
            template_data = {
                'op_kwargs': 'forward=forward',
                'final_state_func': '',
            }

            if custom:
                template_data['op_kwargs'] = 'forward=forward, final_state=final_state'
                writer = MigrationWriter(None, None, db)
                final_state_func = writer._get_define_entities_block(
                        set(), func_name='final_state')
                final_state_func = '\n'.join(('', final_state_func, ''))
                template_data['final_state_func'] = final_state_func

            imports = {
                'from pony.orm import *',
                'from pony.migrate import diagram_ops as op',
            }
            database, im = serializer_factory(db).serialize()
            imports.update(im)
            imports.discard('from pony import orm')

            body = dedent('''
            def forward(db):
                pass
            {final_state_func}
            operations = [
                op.Custom({op_kwargs})
            ]
            ''').format(**template_data)
            imports = sorted(imports, key=lambda i: i.split()[1])
            imports = '\n'.join(imports)
            ctx = {
                'database': database,
                'body': body,
                'imports': imports,
                'deps': str(leaves),
                'version': pony.__version__,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
            generated = MIGRATION_TEMPLATE.format(**ctx)
            p = os.path.join(migrations, '{}.py'.format(name))
            with open(p, 'w') as f:
                f.write(generated)
            print('Written: %s' % os.path.relpath(p))
            return

        writer = MigrationWriter(leaves, prev_db, db)
        generated = writer.as_string()
        if not writer.operations:
            print('No changes.')
            return

        p = os.path.join(migrations, '{}.py'.format(name))
        with open(p, 'w') as f:
            f.write(generated)
        print('Written: %s' % os.path.relpath(p))

    @classmethod
    def _read_db(cls, name, orig_db, db_cache=None):
        if db_cache is None:
            # cache will not be used
            db_cache = {}
        db = db_cache.get(name)
        if db:
            return db
        db = reconstruct_db(orig_db)
        namespace = run_migration_file(name)
        define_entities = namespace['define_entities']
        get_cmd_exitstack().callback(db.disconnect)
        define_entities(db)
        make_migration_entity(db)
        db_cache[name] = db
        return db

    @classmethod
    def apply(cls, db, dry_run=False, is_fake=False, name_start=None, name_end=None,
              name_exact=None):
        get_cmd_exitstack().callback(db.disconnect)
        # temporary, for tests
        # here should be assert db.schema
        if not db.schema:
            db.schema = db.generate_schema()

        assert not 'Migration' in db.entities
        migration_db = reconstruct_db(db)
        make_migration_entity(migration_db)
        migration_db.generate_mapping(create_tables=True, check_tables=True)
        get_cmd_exitstack().callback(migration_db.disconnect)

        graph = MigrationGraph()
        leaves = graph.leaf_nodes()
        if len(leaves) > 1:
            throw(UnmergedMigrationsDetected, leaves)
        assert leaves
        leaf = leaves[0]
        forwards_plan = graph.forwards_plan(leaf)
        names = forwards_plan
        if name_exact:
            start_index = names.index(name_exact)
            names = [names[start_index]]
        elif name_end:
            start_index = names.index(name_start) if name_start else None
            names = names[start_index:names.index(name_end)+1]

        applied = []
        with ExitStack() as stack:
            stack.enter_context(orm.db_session)
            if dry_run:
                stack.enter_context(suppress(orm.DatabaseError))
            applied = orm.select(m.name for m in migration_db.Migration)[:]
            applied = sorted(applied, key=lambda name: forwards_plan.index(name))

        if all(name in applied for name in names):
            print('\nAll migrations are applied.')
            return

        prev_db = reconstruct_db(db)
        new_db = reconstruct_db(db)
        get_cmd_exitstack().callback(prev_db.disconnect)
        get_cmd_exitstack().callback(new_db.disconnect)

        prev_mod = None

        if applied:
            for i, name in enumerate(applied):
                mod = run_migration_file(name)
                if i:
                    upgrade_db(prev_db, prev_mod)
                upgrade_db(new_db, mod)
                prev_mod = mod

        op_batches = []
        applied_names = []

        def generate(new_db, prev_db, entity_ops, current_schema=db.schema):
            current_schema = db.schema
            schema = new_db.generate_schema()
            prev_schema = prev_db and prev_db.generate_schema()

            # TODO remove ?
            operations = [o for batch in op_batches for o in batch]

            from pony.migrate.executor import Executor
            ops = Executor(schema, prev_schema,
                           prev_db=prev_db, db=new_db, current_schema=current_schema,
                           entity_ops=entity_ops, operations=operations
                           )
            return ops.generate()

        def split_ops(entity_ops):
            group = []
            for op in entity_ops:
                if op.is_custom:
                    if group:
                        yield group
                        group = []
                    yield [op]

                else:
                    group.append(op)
            if group:
                yield group

        def execute(ops, db):
            if len(ops) == 1 and isinstance(ops[0], CustomOp):
                with orm.db_session:
                    ops[0].func(db)
                return
            for op in ops:
                cls.execute(op, db, dry_run)

        for i, name in enumerate(names):
            if name in applied:
                continue
            mod = run_migration_file(name)
            if mod.get('operations') is None:
                batches = [mod]
            else:
                batches = [
                    {'operations': ops} for ops in split_ops(mod['operations'])
                ]
            for dic in batches:
                if prev_mod is not None:
                    upgrade_db(prev_db, prev_mod)
                upgrade_db(new_db, dic)
                prev_mod = dic

                if dic.get('operations') and dic['operations'][0].is_custom:
                    ops = dic['operations']
                    op = CustomOp(ops[0].forward)
                    to_execute = [op]
                else:
                    to_execute = generate(new_db, prev_db, dic.get('operations'))

                if not (is_fake and name == '0001_initial'):
                    # execute in transaction
                    @orm.db_session(ddl=True)
                    def run():
                        prev_db_schema = prev_db.schema
                        # FIXME is generated 2nd time
                        prev_db.schema = prev_db.generate_schema()
                        try:
                            execute(to_execute, prev_db)
                        finally:
                            prev_db.schema = prev_db_schema

                    run()

            applied_names.append(name)

        with orm.db_session:
            if not dry_run:
                for name in applied_names:
                    migration_db.Migration(name=name, applied=datetime.now())

        print('Applied:')
        for name in applied_names:
            print('  - %s' % name)


    @classmethod
    def execute(cls, op, db, dry_run=False, applied_ops=None):
        sql = op.get_sql().strip()
        if not sql:
            return
        if not dry_run:
            db.execute(sql)
        else:
            print(sql)


RECURSION_DEPTH_WARNING = (
    "Maximum recursion depth exceeded while generating migration graph, "
    "falling back to iterative approach."
)


@total_ordering
class Node(object):
    """
    A single node in the migration graph. Contains direct links to adjacent
    nodes in either direction.
    """
    def __init__(self, key):
        self.key = key
        self.children = set()
        self.parents = set()
        self._ancestors = None
        self._descendants = None

    def __eq__(self, other):
        return self.key == other

    def __lt__(self, other):
        return self.key < other

    def __hash__(self):
        return hash(self.key)

    def __getitem__(self, item):
        return self.key[item]

    def __unicode__(self):
        return str(self.key)

    def __str__(self):
        return str(self.key).encode('utf-8') if PY2 else str(self.key)

    def __repr__(self):
        return '<Node: (%r, %r)>' % self.key

    def add_child(self, child):
        self.children.add(child)

    def add_parent(self, parent):
        self.parents.add(parent)

    def ancestors(self):
        if self._ancestors is None:
            ancestors = deque([self.key])
            for parent in sorted(self.parents):
                ancestors.extendleft(reversed(parent.ancestors()))
            self._ancestors = list(OrderedDict.fromkeys(ancestors).keys())
        return self._ancestors

    def descendants(self):
        if self._descendants is None:
            descendants = deque([self.key])
            for child in sorted(self.children):
                descendants.extendleft(reversed(child.descendants()))
            self._descendants = list(OrderedDict.fromkeys(descendants).keys())
        return self._descendants


class DummyNode(Node):
    def __init__(self, key, origin, error_message):
        super(DummyNode, self).__init__(key)
        self.origin = origin
        self.error_message = error_message

    def __repr__(self):
        return '<DummyNode: (%r, %r)>' % self.key

    def promote(self):
        """
        Transition dummy to a normal node and clean off excess attribs.
        Creating a Node object from scratch would be too much of a
        hassle as many dependendies would need to be remapped.
        """
        del self.origin
        del self.error_message
        self.__class__ = Node

    def raise_error(self):
        raise NodeNotFoundError(self.error_message, self.key, origin=self.origin)


class MigrationGraph(object):
    """
    Represents the digraph of all migrations in a project.

    Each migration is a node, and each dependency is an edge. There are
    no implicit dependencies between numbered migrations - the numbering is
    merely a convention to aid file listing. Every new numbered migration
    has a declared dependency to the previous number, meaning that VCS
    branch merges can be detected and resolved.

    Migrations files can be marked as replacing another set of migrations -
    this is to support the "squash" feature. The graph handler isn't responsible
    for these; instead, the code to load them in here should examine the
    migration files and if the replaced migrations are all either unapplied
    or not present, it should ignore the replaced ones, load in just the
    replacing migration, and repoint any dependencies that pointed to the
    replaced migrations to point to the replacing one.

    A node should be a tuple: (app_path, migration_name). The tree special-cases
    things within an app - namely, root nodes and leaf nodes ignore dependencies
    to other apps.
    """

    def __init__(self):
        self.disk_migrations = {}
        self.node_map = {}
        self.nodes = {}
        self.cached = False
        self.build_graph()

    def build_graph(self):
        self.load_disk()
        for migration_name, migration in self.disk_migrations.items():
            self.add_node(migration_name, migration)
            for parent in migration.dependencies:
                self.add_dependency(migration, migration_name, parent, skip_validation=True)
        self.validate_consistency()

    def load_disk(self):
        self.disk_migrations = {}
        directory = get_migration_dir()
        if not os.path.exists(directory):
            return

        migration_files = glob(os.path.join(directory, '*.py'))
        for migration_pathname in migration_files:
            namespace = run_path(migration_pathname)
            migration_name = os.path.basename(migration_pathname)[:-3]
            migration = Migration(migration_name, self)
            if 'dependencies' not in namespace: throw(MigrationFileCorrupted,
                '`dependencies` list was not found in migration file %s' % migration_pathname)
            migration.dependencies = namespace['dependencies']
            self.disk_migrations[migration_name] = migration

    def add_node(self, key, migration):
        # If the key already exists, then it must be a dummy node.
        dummy_node = self.node_map.get(key)
        if dummy_node:
            # Promote DummyNode to Node.
            dummy_node.promote()
        else:
            node = Node(key)
            self.node_map[key] = node
        self.nodes[key] = migration
        self.clear_cache()

    def add_dummy_node(self, key, origin, error_message):
        node = DummyNode(key, origin, error_message)
        self.node_map[key] = node
        self.nodes[key] = None

    def add_dependency(self, migration, child, parent, skip_validation=False):
        """
        This may create dummy nodes if they don't yet exist.
        If `skip_validation` is set, validate_consistency should be called afterwards.
        """
        if child not in self.nodes:
            error_message = (
                "Migration %s dependencies reference nonexistent"
                " child node %r" % (migration, child)
            )
            self.add_dummy_node(child, migration, error_message)
        if parent not in self.nodes:
            error_message = (
                "Migration %s dependencies reference nonexistent"
                " parent node %r" % (migration, parent)
            )
            self.add_dummy_node(parent, migration, error_message)
        self.node_map[child].add_parent(self.node_map[parent])
        self.node_map[parent].add_child(self.node_map[child])
        if not skip_validation:
            self.validate_consistency()
        self.clear_cache()

    def remove_replaced_nodes(self, replacement, replaced):
        """
        Removes each of the `replaced` nodes (when they exist). Any
        dependencies that were referencing them are changed to reference the
        `replacement` node instead.
        """
        # Cast list of replaced keys to set to speed up lookup later.
        replaced = set(replaced)
        try:
            replacement_node = self.node_map[replacement]
        except KeyError as exc:
            exc_value = NodeNotFoundError(
                "Unable to find replacement node %r. It was either never added"
                " to the migration graph, or has been removed." % (replacement, ),
                replacement
            )
            exc_value.__cause__ = exc
            if not hasattr(exc, '__traceback__'):
                exc.__traceback__ = sys.exc_info()[2]
            reraise(NodeNotFoundError, exc_value, sys.exc_info()[2])
        for replaced_key in replaced:
            self.nodes.pop(replaced_key, None)
            replaced_node = self.node_map.pop(replaced_key, None)
            if replaced_node:
                for child in replaced_node.children:
                    child.parents.remove(replaced_node)
                    # We don't want to create dependencies between the replaced
                    # node and the replacement node as this would lead to
                    # self-referencing on the replacement node at a later iteration.
                    if child.key not in replaced:
                        replacement_node.add_child(child)
                        child.add_parent(replacement_node)
                for parent in replaced_node.parents:
                    parent.children.remove(replaced_node)
                    # Again, to avoid self-referencing.
                    if parent.key not in replaced:
                        replacement_node.add_parent(parent)
                        parent.add_child(replacement_node)
        self.clear_cache()

    def remove_replacement_node(self, replacement, replaced):
        """
        The inverse operation to `remove_replaced_nodes`. Almost. Removes the
        replacement node `replacement` and remaps its child nodes to
        `replaced` - the list of nodes it would have replaced. Its parent
        nodes are not remapped as they are expected to be correct already.
        """
        self.nodes.pop(replacement, None)
        try:
            replacement_node = self.node_map.pop(replacement)
        except KeyError as exc:
            exc_value = NodeNotFoundError(
                "Unable to remove replacement node %r. It was either never added"
                " to the migration graph, or has been removed already." % (replacement, ),
                replacement
            )
            exc_value.__cause__ = exc
            if not hasattr(exc, '__traceback__'):
                exc.__traceback__ = sys.exc_info()[2]
            reraise(NodeNotFoundError, exc_value, sys.exc_info()[2])
        replaced_nodes = set()
        replaced_nodes_parents = set()
        for key in replaced:
            replaced_node = self.node_map.get(key)
            if replaced_node:
                replaced_nodes.add(replaced_node)
                replaced_nodes_parents |= replaced_node.parents
        # We're only interested in the latest replaced node, so filter out
        # replaced nodes that are parents of other replaced nodes.
        replaced_nodes -= replaced_nodes_parents
        for child in replacement_node.children:
            child.parents.remove(replacement_node)
            for replaced_node in replaced_nodes:
                replaced_node.add_child(child)
                child.add_parent(replaced_node)
        for parent in replacement_node.parents:
            parent.children.remove(replacement_node)
            # NOTE: There is no need to remap parent dependencies as we can
            # assume the replaced nodes already have the correct ancestry.
        self.clear_cache()

    def validate_consistency(self):
        """
        Ensure there are no dummy nodes remaining in the graph.
        """
        [n.raise_error() for n in self.node_map.values() if isinstance(n, DummyNode)]

    def clear_cache(self):
        if self.cached:
            for node in self.nodes:
                self.node_map[node]._ancestors = None
                self.node_map[node]._descendants = None
            self.cached = False

    def forwards_plan(self, target):
        """
        Given a node, returns a list of which previous nodes (dependencies)
        must be applied, ending with the node itself.
        This is the list you would follow if applying the migrations to
        a database.
        """
        if target not in self.nodes:
            raise NodeNotFoundError("Node %r not a valid node" % (target, ), target)
        # Use parent.key instead of parent to speed up the frequent hashing in ensure_not_cyclic
        self.ensure_not_cyclic(target, lambda x: (parent.key for parent in self.node_map[x].parents))
        self.cached = True
        node = self.node_map[target]
        try:
            return node.ancestors()
        except RuntimeError:
            # fallback to iterative dfs
            warnings.warn(RECURSION_DEPTH_WARNING, RuntimeWarning)
            return self.iterative_dfs(node)

    def backwards_plan(self, target):
        """
        Given a node, returns a list of which dependent nodes (dependencies)
        must be unapplied, ending with the node itself.
        This is the list you would follow if removing the migrations from
        a database.
        """
        if target not in self.nodes:
            raise NodeNotFoundError("Node %r not a valid node" % (target, ), target)
        # Use child.key instead of child to speed up the frequent hashing in ensure_not_cyclic
        self.ensure_not_cyclic(target, lambda x: (child.key for child in self.node_map[x].children))
        self.cached = True
        node = self.node_map[target]
        try:
            return node.descendants()
        except RuntimeError:
            # fallback to iterative dfs
            warnings.warn(RECURSION_DEPTH_WARNING, RuntimeWarning)
            return self.iterative_dfs(node, forwards=False)

    def iterative_dfs(self, start, forwards=True):
        """
        Iterative depth first search, for finding dependencies.
        """
        visited = deque()
        visited.append(start)
        if forwards:
            stack = deque(sorted(start.parents))
        else:
            stack = deque(sorted(start.children))
        while stack:
            node = stack.popleft()
            visited.appendleft(node)
            if forwards:
                children = sorted(node.parents, reverse=True)
            else:
                children = sorted(node.children, reverse=True)
            # reverse sorting is needed because prepending using deque.extendleft
            # also effectively reverses values
            stack.extendleft(children)

        return list(OrderedDict.fromkeys(visited).keys())

    def root_nodes(self, app=None):
        """
        Returns all root nodes - that is, nodes with no dependencies inside
        their app. These are the starting point for an app.
        """
        roots = set()
        for node in self.nodes:
            if not any(key[0] == node[0] for key in self.node_map[node].parents) and (not app or app == node[0]):
                roots.add(node)
        return sorted(roots)

    def leaf_nodes(self, app=None):
        """
        Returns all leaf nodes - that is, nodes with no dependents in their app.
        These are the "most current" version of an app's schema.
        Having more than one per app is technically an error, but one that
        gets handled further up, in the interactive command - it's usually the
        result of a VCS merge and needs some user input.
        """
        leaves = set()
        for node in self.nodes:
            if not any(key[0] == node[0] for key in self.node_map[node].children) and (not app or app == node[0]):
                leaves.add(node)
        return sorted(leaves)

    def ensure_not_cyclic(self, start, get_children):
        # Algo from GvR:
        # http://neopythonic.blogspot.co.uk/2009/01/detecting-cycles-in-directed-graph.html
        todo = set(self.nodes)
        while todo:
            node = todo.pop()
            stack = [node]
            while stack:
                top = stack[-1]
                for node in get_children(top):
                    if node in stack:
                        cycle = stack[stack.index(node):]
                        raise CircularDependencyError(", ".join("%s.%s" % n for n in cycle))
                    if node in todo:
                        stack.append(node)
                        todo.remove(node)
                        break
                else:
                    node = stack.pop()

    def __unicode__(self):
        return 'Graph: %s nodes, %s edges' % self._nodes_and_edges()

    def __str__(self):
        return self.__unicode__().encode('utf-8') if PY2 else self.__unicode__()

    def __repr__(self):
        nodes, edges = self._nodes_and_edges()
        return '<%s: nodes=%s, edges=%s>' % (self.__class__.__name__, nodes, edges)

    def _nodes_and_edges(self):
        return len(self.nodes), sum(len(node.parents) for node in self.node_map.values())

    def __contains__(self, node):
        return node in self.nodes
