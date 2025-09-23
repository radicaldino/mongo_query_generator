from pymongo import MongoClient
from .settings import settings
from typing import Dict

_clients: dict[str, MongoClient] = {}
_db_names: dict[str, str] = {}

def _parse_mapping(s: str | None) -> dict[str, str]:
    if not s:
        return {}
    mapping: dict[str, str] = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        alias, _, value = part.partition(":")
        if not alias or not value:
            continue
        mapping[alias.strip()] = value.strip()
    return mapping

def _init_multi() -> None:
    global _clients, _db_names
    if _clients:
        return
    uris = _parse_mapping(settings.MONGO_URIS)
    dbs = _parse_mapping(settings.MONGO_DBS)
    for alias, uri in uris.items():
        _clients[alias] = MongoClient(
            uri,
            appname=f"ezscm-mcp-{alias}",
            serverSelectionTimeoutMS=5000,
            maxPoolSize=50,
            retryWrites=True,
        )
    _db_names = dbs

def get_db(alias:str):
    _init_multi()
    if alias not in _clients:
        raise ValueError(f"Unknown DB alias '{alias}'. Known: {list(_clients.keys())}")
    if alias not in _db_names:
        raise ValueError(f"No DB name configured for alias '{alias}'.")
    return _clients[alias][_db_names[alias]]

def get_db_aliases() -> list[str]:
    """Return configured DB aliases without touching pymongo clients."""
    return list(MONGO_DBS.keys())

def _parse_alias_db_map(s: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in (s or "").split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        alias, db = part.split(":", 1)
        alias, db = alias.strip(), db.strip()
        if alias and db:
            out[alias] = db
    return out

# Build MONGO_DBS from env, fallback to hardcoded defaults if needed
_MONGO_DBS_ENV = _parse_alias_db_map(settings.MONGO_DBS or "")
MONGO_DBS: Dict[str, str] = _MONGO_DBS_ENV or {
    "Inventory": "xFlowInventory",
    "App": "xFlowApp",
    "Component": "xFlowComponent",
    "Fulfillment": "ezSCMFulfilment",
}

DB_TO_ALIAS: Dict[str, str] = {db: alias for alias, db in MONGO_DBS.items()}

def alias_for_db(db_name: str) -> str:
    return DB_TO_ALIAS.get(db_name, db_name)

def db_name_for_alias(alias: str) -> str:
    return MONGO_DBS.get(alias, alias)