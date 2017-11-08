from datetime import datetime
from pony.orm import *


db = Database()


class BaseBridge(db.Entity):
    _table_ = 'MT4_BASE_BRIDGE'
    id = PrimaryKey(int, auto=True)
    name = Required(str, unique=True)
    bridge_params = Set('BridgeParamValue')


class Symbol(db.Entity):
    _table_ = 'MT4_SYMBOL'
    name = PrimaryKey(unicode)
    index = Optional(int)
    pip_price = Optional(float)
    values = Set('SymbolValue')


class Bridge(BaseBridge):
    bridge_group = Required('BridgeGroup')
    user_id = Optional(int, unique=True)
    fxco_login = Optional(str, unique=True)
    symbol_group = Required('SymbolGroup')
    broker_params = Set('BrokerConfig')
    mam_params = Set('MAMConfig')
    equity_stop_params = Set('EquityStopConfig')
    mt_server = Optional('MTServer')
    server = Optional('Server')


class BrokerConfig(db.Entity):
    _table_ = 'MT4_BROKER_CONFIG'
    id = PrimaryKey(int, auto=True)
    broker_account = Required(str)
    groups = Required(str)
    bridge = Required(Bridge)


class BridgeGroup(BaseBridge):
    bridges = Set(Bridge)


class BridgeParamValue(db.Entity):
    _table_ = 'MT4_BRIDGE_PARAM_VALUE'
    id = PrimaryKey(int, auto=True)
    bridge = Required(BaseBridge)
    param = Required('BridgeParam')
    value = Optional(str)
    access_level = Required(int, default=0)


class SymbolValue(db.Entity):
    _table_ = 'MT4_SYMBOL_VALUE'
    id = PrimaryKey(int, auto=True)
    symbol = Required(Symbol)
    param = Required('SymbolParam')
    value = Optional(str)
    symbol_group = Required('SymbolGroup')


class SymbolParam(db.Entity):
    _table_ = 'MT4_SYMBOL_PARAM'
    name = PrimaryKey(unicode)
    index = Optional(int)
    values = Set(SymbolValue)


class BridgeParam(db.Entity):
    _table_ = 'MT4_BRIDGE_PARAM'
    name = PrimaryKey(str)
    values = Set(BridgeParamValue)


class MAMConfig(db.Entity):
    _table_ = 'MT4_MAM_CONFIG'
    id = PrimaryKey(int, auto=True)
    bridge = Required(Bridge)
    broker_login = Required(int)
    master_group = Required(str)
    subaccount_group = Required(str)


class EquityStopConfig(db.Entity):
    _table_ = 'MT4_EQUITY_STOP_CONFIG'
    id = PrimaryKey(int, auto=True)
    bridge = Required(Bridge)
    param = Required(str)
    value = Required(float)
    type = Required(str)


class SymbolGroup(db.Entity):
    _table_ = 'MT4_SYMBOL_GROUP'
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    symbol_values = Set(SymbolValue)
    bridges = Set(Bridge)


class User(db.Entity):
    _table_ = 'MT4_MTCONSOLE_USER'
    id = PrimaryKey(int, auto=True)
    username = Required(str)
    password = Required(str)
    name = Required(str)
    email = Required(str)
    status = Required(str, default='enabled')
    access_level = Required(int, default=0)
    admin = Required(bool, default=False)
    log_records = Set('LogRecord')


class LogRecord(db.Entity):
    _table_ = 'MT4_LOG_RECORD'
    id = PrimaryKey(int, auto=True)
    dt = Required(datetime, default=datetime.now)
    user = Required(User)
    action = Required(str)
    obj_class = Optional(str)
    obj_id = Optional(int)
    text = Optional(str)
    json_data = Optional(str)
    ip = Optional(str)
    browser = Optional(str)


class ASM_USER(db.Entity):
    id = PrimaryKey(int, auto=True)
    user_id = Required(int)
    login_name = Required(str)


class MTServer(db.Entity):
    _table_ = 'MT4_MTSERVER'
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    host = Required(str)
    info = Optional(str)
    bridges = Set(Bridge)


class Server(db.Entity):
    _table_ = 'MT4_SERVER'
    id = PrimaryKey(int, auto=True)
    host = Required(str)
    info = Optional(str)
    bridges = Set(Bridge)


db.connect("sqlite", ":memory:", create_db=True, create_tables=True)