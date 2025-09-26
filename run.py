# run.py
import asyncio, os
from contextlib import AsyncExitStack
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
<<<<<<< HEAD
from mcp_server.mcp_session_global import set_mcp_session
=======
from mcp_server.main import set_mcp_session
>>>>>>> 223c614a58b7b3575c30fb085cd8d3454533adf0
from langgraph_agent.graph import build_graph
from config.settings import settings
from config.database import _parse_mapping
from anyio import BrokenResourceError

MCP_CMD = "/usr/local/bin/mongodb-mcp-server"

# Prefer per-alias env; fallback to settings.MONGO_URIS mapping
MCP_URIS = {
    "Inventory": os.getenv("MDB_MCP_CONNECTION_STRING_INVENTORY", ""),
    "App": os.getenv("MDB_MCP_CONNECTION_STRING_APP", ""),
    "Component": os.getenv("MDB_MCP_CONNECTION_STRING_COMPONENT", ""),
    "Fulfillment": os.getenv("MDB_MCP_CONNECTION_STRING_FULFILLMENT", ""),
}
if not any(MCP_URIS.values()):
    MCP_URIS = _parse_mapping(settings.MONGO_URIS or "")

async def run_async(user_query: str):
    os.makedirs("./logs", exist_ok=True)
    ret = None
    try:
        async with AsyncExitStack() as stack:
            sessions = {}
            for alias, uri in MCP_URIS.items():
                if not uri:
                    continue
                env = os.environ.copy()
                env["MDB_MCP_CONNECTION_STRING"] = uri
                env.setdefault("MDB_MCP_LOG_PATH", f"./logs/mongodb-mcp-{alias.lower()}.log")
                params = StdioServerParameters(command=MCP_CMD, args=[], env=env)

                read, write = await stack.enter_async_context(stdio_client(params))
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()

                set_mcp_session(alias, session)
                sessions[alias] = session

            if not sessions:
                raise RuntimeError(f"No MCP sessions registered. URIs: {MCP_URIS}")

            print(f"Registered MCP sessions: {list(sessions.keys())}")
            graph = build_graph()
            ret = await graph.ainvoke({"input": user_query}, return_state=True)
            # tiny drain to let logs flush before shutdown
            await asyncio.sleep(0.05)
            return ret
    except* BrokenResourceError:
        # benign during stdio shutdown
        pass
    return ret

def run(user_query: str):
    return asyncio.run(run_async(user_query))

if __name__ == "__main__":
    out = run("Show me all the dates of the new materials created ")
    if isinstance(out, dict):
        print("Final result:", out.get("result") or out.get("query") or out)
    else:
        print("Final result:", out)
