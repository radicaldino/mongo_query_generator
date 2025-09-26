from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import anyio


class McpSessionManager:
    def __init__(self, alias_env: dict[str, dict[str, str]]):
        self.alias_env = alias_env
        self.sessions: dict[str, tuple] = {}

    async def start(self):
        for alias, env in self.alias_env.items():
            if not env.get("MDB_MCP_CONNECTION_STRING"):
                continue
            params = StdioServerParameters(command="/usr/local/bin/mongodb-mcp-server", args=[], env=env)
            ctx = stdio_client(params)
            read, write = await ctx.__aenter__()
            session = ClientSession(read, write)
            await session.__aenter__()
            await session.initialize()
            self.sessions[alias] = (ctx, session)

    async def stop(self):
        for ctx, session in self.sessions.values():
            try:
                await session.__aexit__(None, None, None)
            except Exception:
                pass
            try:
                await ctx.__aexit__(None, None, None)
            except Exception:
                pass
        self.sessions.clear()

    def get(self, alias: str) -> ClientSession:
        if alias not in self.sessions:
            raise RuntimeError(f"No MCP session for alias '{alias}'")
        return self.sessions[alias][1]

    async def reconnect(self, alias: str) -> ClientSession:
        if alias in self.sessions:
            ctx, session = self.sessions.pop(alias)
            with anyio.CancelScope(shield=True):
                try: await session.__aexit__(None, None, None)
                except Exception: pass
                try: await ctx.__aexit__(None, None, None)
                except Exception: pass
        env = self.alias_env.get(alias) or {}
        if not env.get("MDB_MCP_CONNECTION_STRING"):
            raise RuntimeError(f"No MCP connection string for alias '{alias}'")
        params = StdioServerParameters(command="/usr/local/bin/mongodb-mcp-server", args=[], env=env)
        ctx = stdio_client(params)
        read, write = await ctx.__aenter__()
        session = ClientSession(read, write)
        await session.__aenter__()
        await session.initialize()
        self.sessions[alias] = (ctx, session)
        return session

