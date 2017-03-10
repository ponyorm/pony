#!/usr/bin/env python

from app import settings
from app.models import db

db.migrate(**settings.db_params)
