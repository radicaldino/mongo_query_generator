from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import anyio
import os
import logging
from typing import Dict, Optional, Any
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class RequestScopedSessionManager:
    """Per-request MCP session manager for isolation and proper cleanup"""
    
    def __init__(self, alias_env: Dict[str, Dict[str, str]]):
        self.alias_env = alias_env
        self.sessions: Dict[str, tuple] = {}
        self._active = False

    async def start(self, aliases: Optional[list] = None):
        """Start sessions for configured aliases (optionally limited subset)"""
        if self._active:
            return
        targets = aliases or list(self.alias_env.keys())
        for alias in targets:
            env = self.alias_env.get(alias) or {}
            if not env.get("MDB_MCP_CONNECTION_STRING"):
                continue
            try:
                await self._create_session(alias, env)
            except Exception as e:
                logger.error(f"Failed to start MCP session for {alias}: {e}")
                continue
        self._active = True
        logger.info(f"Started MCP sessions for aliases: {list(self.sessions.keys())}")
    
    async def stop(self):
        """Stop all sessions and cleanup resources"""
        if not self._active:
            return
            
        for alias, (ctx, session) in self.sessions.items():
            try:
                await session.__aexit__(None, None, None)
            except Exception as e:
                logger.warning(f"Error closing session for {alias}: {e}")
            try:
                await ctx.__aexit__(None, None, None)
            except Exception as e:
                logger.warning(f"Error closing context for {alias}: {e}")
        
        self.sessions.clear()
        self._active = False
        logger.info("All MCP sessions stopped")

    async def _create_session(self, alias: str, env: Dict[str, str]):
        """Create a new MCP session for the given alias"""
        params = StdioServerParameters(
            command="/usr/local/bin/mongodb-mcp-server", 
            args=[], 
            env=env
        )
        ctx = stdio_client(params)
        read, write = await ctx.__aenter__()
        session = ClientSession(read, write)
        await session.__aenter__()
        await session.initialize()
        self.sessions[alias] = (ctx, session)

    def get(self, alias: str) -> ClientSession:
        """Get session for alias"""
        if alias not in self.sessions:
            raise RuntimeError(f"No MCP session for alias '{alias}'")
        return self.sessions[alias][1]

    async def reconnect(self, alias: str) -> ClientSession:
        """Reconnect session for alias"""
        if alias in self.sessions:
            ctx, session = self.sessions.pop(alias)
            with anyio.CancelScope(shield=True):
                try:
                    await session.__aexit__(None, None, None)
                except Exception:
                    pass
                try:
                    await ctx.__aexit__(None, None, None)
                except Exception:
                    pass
        
        env = self.alias_env.get(alias) or {}
        if not env.get("MDB_MCP_CONNECTION_STRING"):
            raise RuntimeError(f"No MCP connection string for alias '{alias}'")
        
        await self._create_session(alias, env)
        return self.sessions[alias][1]

    @asynccontextmanager
    async def session_context(self, aliases: Optional[list] = None):
        """Context manager for per-request session lifecycle"""
        aliases = aliases or list(self.alias_env.keys())
        try:
            # Start only required sessions
            for alias in aliases:
                if alias not in self.sessions and alias in self.alias_env:
                    env = self.alias_env[alias]
                    if env.get("MDB_MCP_CONNECTION_STRING"):
                        await self._create_session(alias, env)
            yield self
        finally:
            # Cleanup only the sessions we created
            for alias in aliases:
                if alias in self.sessions:
                    ctx, session = self.sessions.pop(alias)
                    try:
                        await session.__aexit__(None, None, None)
                    except Exception as e:
                        logger.warning(f"Error closing session for {alias}: {e}")
                    try:
                        await ctx.__aexit__(None, None, None)
                    except Exception as e:
                        logger.warning(f"Error closing context for {alias}: {e}")

# Global factory for creating request-scoped managers
def create_request_session_manager() -> RequestScopedSessionManager:
    """Factory to create a new request-scoped session manager"""
    from config.database import _parse_mapping
    from config.settings import settings
    
    # Get MCP URIs
    MCP_URIS = {
        "Inventory": os.getenv("MDB_MCP_CONNECTION_STRING_INVENTORY", ""),
        "App": os.getenv("MDB_MCP_CONNECTION_STRING_APP", ""),
        "Component": os.getenv("MDB_MCP_CONNECTION_STRING_COMPONENT", ""),
        "Fulfillment": os.getenv("MDB_MCP_CONNECTION_STRING_FULFILLMENT", ""),
    }
    
    if not any(MCP_URIS.values()):
        MCP_URIS = _parse_mapping(settings.MONGO_URIS or "")
    
    # Build environment for each alias
    alias_env: Dict[str, Dict[str, str]] = {}
    for alias, uri in MCP_URIS.items():
        if not uri:
            continue
        env = os.environ.copy()
        env["MDB_MCP_CONNECTION_STRING"] = uri
        env.setdefault("MDB_MCP_LOG_PATH", f"./logs/mongodb-mcp-{alias.lower()}.log")
        alias_env[alias] = env
    
    return RequestScopedSessionManager(alias_env)

