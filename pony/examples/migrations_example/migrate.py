#!/usr/bin/env python

from app.models import db
db.migrate('app/migrations')