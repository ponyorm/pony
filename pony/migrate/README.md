
# Database migrations with Pony.

For those familiar with database migrations in Django, Pony migrations will seem pretty familiar. Database migrations are stored in Python modules, that can be applied (imported and executed), in the `migrations` directory.

## Preconditions

Pony needs entities declarations to auto-generate migrations. Also, it needs `Database` object to connect to database.

By default, Pony will look up the `entities.py` file located in current directory for the entities declaration. Also, that module should have the `db` attribute referencing `Database`. Like this

```python
# entities.py

db = Database(provider, *connection)

class MyEntity(db.Entity):
  ...

db.generate_mapping(create_tables=False, check_tables=False)
```

The name of the module with entities declarations can be customized with the `PONY_ENTITIES` environmental variable.

## The migration command

 The main migration command is

```bash
python -m pony.migrate
```

Actually, Pony also generates `migrate` script for you so you can do

```bash
migrate --help
```

It means the same, only passing `--help` gives you a more detailed usage description.

## Getting started


Usually, performing a migration consists of 2 steps. 1st step is **making the migration**.

```bash
migrate make
```

This will generate the migration file in the `migrations` directory. It will be called `0001_initial.py` if it's the first migration in the project, and something like `0004_20161230_1324.py` if it's not. Migration names usually are prefixed with a number but this is not required.

Suppose the first command produced you `0001_initial.py` file. If you open it, you'll see your entities declarations in it. To create the tables in the database for them, **apply the migration**

```bash
migrate apply
```

If you already have the tables created in your database, you should use the `--fake-initial` option:

 ```bash
migrate apply --fake-initial
```

Now suppose you did some changes to `entities.py` file. Do the procedure again: `migrate make`.

You will notice a new file created, with a name prefixed with `0002`.

You can take a look at its contents, it's pretty self-explanatory. Apply it with `migrate apply`.


## Merging migrations

Sometimes, after merging from other branches from the repository, you can get multiple migration files
depending on a single parent. If you try `migrate make` or `migrate apply` commands, you will be asked if you want
to merge those migrations:

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
migrate list
```

This will list all migrations. The ones marked with `>` are applied.


```bash
migrate sql 0002
```

Shows the sql for the migrations starting with `0002`. It should not be applied yet.

```bash
migrate apply 0001 0003
```

Apply migrations from the one starting with `0001` (inclusive) to the one starting with `0003` (inclusive)