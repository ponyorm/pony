# Database migrations with Pony

Version 0.8 of PonyORM will add migration support. The concept of migrations looks similar to Django. Migrations are stored as a Python files in `migrations` directory. A developer can `make` a new migration file which records a difference between previous migrations and current entity classes, `apply` existing migrations to a database, `merge` migrations from a different code branches, print `sql` of specific migration or display `list` of existing migrations. All these commands can be invoked using `migrate` method of a `Database` object. Also it is possible to write a simple helper file in order to call all these commands from the command line interface. 

**CAUTION:** The code for migration support is not production-ready yet! It may contains multiple bugs, and complex use-cases are not supported yet. It is here only to demonstrate and discuss the migration API.


## Backward-incompatible changes

PonyORM version 0.8 contains some backward-incompatible changes. Python code is compatible with previous Pony versions, but table and column names are sometimes different. These changes are necessary in order to eliminate some ambiguity which can prevent correct working with migrations:  

  * The default names for tables which represent many-to-many relationships were changed. The previous names looks like `entity1_entity2`, and new name looks like `entity1_attr1`. This change is necessary in order to prevent ambiguity when there are more than one many-to-many relationships between two entities.

  * The default names of tables, columns, foreign keys and indexes which are too long to be represented in database now end with a hash codes. It is necessary to prevent a situation when two long names becomes non-distinguishable when cutting to database length limit for identifiers. 

In order to use previously created databases, Pony needs to upgrade the database schema by performing renames. After the upgrade you will not be able to use any previous versions of Pony with that database. Because of this it is strongly advised to make a database backup before starting experiments with migrations.
 
 Pony can upgrade the database schema automatically, but needs an explicit permission to do it. In order to give that permission, you need to specify `allow_auto_upgrade=True` keyword argument in `db.generate_mapping(...)` call:

```python
db.generate_mapping(allow_auto_upgrade=True)
```

Without `allow_auto_upgrade=True` option Pony will throw an `UpgradeError` exception, the exception message will specify which tables/columns will be renamed during upgrade.

If you use raw SQL queries which refer to renamed table or column names, you also need to modify your SQL code manually.

During the upgrade Pony will create new table `pony_version` with a single row which keep the version of Pony with which the tables were created or upgraded. It should make future upgrades easier, if they will be necessary. 

After the upgrade is successfully applied you can remove `allow_auto_upgrade=True` option from `generate_mapping` method arguments.


## New database API methods

In previous Pony versions it was necessary to sequentially execute two methods of `Database` object in order to connect to the database server: at first `bind` method to specify connection options such as username and password, and then `generate_mapping` method which determines what tables should be presented in the database. Now it is possible to use a single method `connect` which combines both actions. So, instead of

```python
db.bind(**settings.db_params)
db.generate_mapping(allow_auto_upgrade=True)
```

you can write just:

```python
db.connect(allow_auto_upgrade=True, **settings.db_params)
```

If you previously specified `create_tables=True` option in your code, you need to remove it, because with migrations you will create or alter tables by using migration `apply` command.

The second of new `Database` methods is named `migrate`. It can accept the same arguments as `connect`, so you can write:

```python
db.migrate(**settings.db_params)
```

The `migrate` method can accept `create_tables` and `check_tables` options, but they are ignored silently. It also accepts two optional keyword arguments: `command` option which specifies a migration command and `migration_dir` option which specifies are path to migrations directory. So you can write, for example:

```python
db.migrate(command='make', migration_dir='migrations', **settings.db_params)
```

The relative path `'migrations'` is the default value of `migration_dir`. 

It may be convenient to specify `migration_dir` option together with other connection options inside `db_params`. The `connect` method will ignore it silently, so you can pass the same set of connection options to both `connect` and `migrate` methods. 

Another way to specify migration directory is to set `MIGRATIONS_DIR` environment variable.

When `command` option is missed, the command is parsed from `sys.argv` command line arguments. It make it very easy to write migration script as specified in the following section.


## Using command line interface 

It is very easy to write a script for using migrations from a command line. It may look like the following example of `migrate.py` script:

```python
#!/usr/bin/env python
import models
import settings

models.db.migrate(**settings.db_params)
```

If you place that script in the directory with your project, you can invoke migration command by calling that script, for example `migrate.py make`. On Windows you need to specify python interpreter too: `python migrate.py make`. Now, after we know how to write a migration script, we can take a more detailed look at a migration commands. 


## Migration commands


Usually, performing a migration consists of 2 steps. 1st step is **making the migration**.

```bash
migrate.py make
```

This will generate the migration file in the `migrations` directory. It will be called `0001_initial.py` if it's the first migration in the project, and something like `0004_20161230_1324.py` if it's not. Migration names usually are prefixed with a number but this is not required.

Suppose the first command produced you `0001_initial.py` file. If you open it, you'll see your entity declarations in it. To create the tables in the database for them, **apply the migration**

```bash
migrate.py apply
```

If you already have the tables created in your database, you should use the `--fake-initial` option:

 ```bash
migrate.py apply --fake-initial
```

Now suppose you did some changes to `entities.py` file. Do the procedure again: `migrate.py make`.

You will notice a new file created, with a name prefixed with `0002`.

You can take a look at its contents, it's pretty self-explanatory. Apply it with `migrate.py apply`.


## Merging migrations

Sometimes, after merging from other branches from the repository, you can get multiple migration files
depending on a single parent. If you try `migrate make` or `migrate apply` commands, you will be asked if you want to merge those migrations:

```
Merging: 0002_20170123_1127, 0002_20170123_1134

Merging will only work if the operations printed above do not conflict
with each other (working on different fields or models)
Do you want to merge these migrations? [y/N]
```


If you agree, an "empty" migration will be produced, having those two 0002-prefixed files as its dependencies, thus restoring a single leaf in your migration graph.


## Making a data migration

Sometimes you need to make a custom migration, done with a script (or function). Usually, that is the case when you modify the data in the database, not its schema.

For this, execute

```bash
migrate make --empty
```

This will produce file with following contents:

```python
from pony.migrate import diagram_ops as op
from pony.orm import *

dependencies = ['0002_20161230_1610']


def forward(db):
    pass

operations = [
    op.Custom(forward=forward)
]
```

Notice the `forward` function. That is the one that will contain the logic for your migration. For example:

```python
def forward(db):
    delete(d for d in Data if d.is_obsolete)
```

## Other commands

```bash
migrate.py list
```

This will list all migrations. The ones marked with `+` are applied.


```bash
migrate.py sql 0002
```

Shows the sql for the migrations starting with `0002`. It should not be applied yet.

```bash
migrate.py apply 0001 0003
```

Apply migrations from the one starting with `0001` (inclusive) to the one starting with `0003` (inclusive)
