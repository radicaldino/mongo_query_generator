# mcp_server/main.py
from config.database import get_db_aliases
from mcp import ClientSession, StdioServerParameters
from mcp_server.tools.report_tools import ReportTools
import asyncio

# Global variable to store MCP sessions
_mcp_sessions = {}

def set_mcp_session(alias: str, session):
    _mcp_sessions[alias] = session

def get_mcp_session(alias: str):
    if alias not in _mcp_sessions:
        raise ValueError(f"MCP session for alias '{alias}' not found. Registered: {list(_mcp_sessions.keys())}")
    return _mcp_sessions[alias]
