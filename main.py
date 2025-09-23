import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from apis.api import router as api_router
from config.settings import settings 
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import os
from mcp_server.main import set_mcp_session
import logging, os
from config.database import _parse_mapping


# Initialize LangSmith
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = settings.LANGSMITH_PROJECT
os.environ["LANGCHAIN_API_KEY"] = settings.LANGSMITH_API_KEY


# use settings.PORT
PORT = settings.PORT


# If you want to ensure the logs dir exists
os.makedirs("./logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler("./logs/agent.log"),
        logging.StreamHandler()
    ],
    force=True,
)   ,
    

# define your alias → URI (from your env/config)
MCP_URIS = {
    "Inventory": os.getenv("MDB_MCP_CONNECTION_STRING_INVENTORY", ""),
    "App": os.getenv("MDB_MCP_CONNECTION_STRING_APP", ""),
    "Component": os.getenv("MDB_MCP_CONNECTION_STRING_COMPONENT", ""),
    "Fulfillment": os.getenv("MDB_MCP_CONNECTION_STRING_FULFILLMENT", ""),
}
if not any(MCP_URIS.values()):
    MCP_URIS = _parse_mapping(settings.MONGO_URIS or "")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mcp_sessions = {}
    app.state.mcp_contexts = []  # keep contexts alive
    for alias, uri in MCP_URIS.items():
        if not uri:
            continue
        env = os.environ.copy()
        env["MDB_MCP_CONNECTION_STRING"] = uri
        env.setdefault("MDB_MCP_LOG_PATH", f"./logs/mongodb-mcp-{alias.lower()}.log")
        params = StdioServerParameters(command="/usr/local/bin/mongodb-mcp-server", args=[], env=env)
        ctx = stdio_client(params)
        app.state.mcp_contexts.append(ctx)
        read, write = await ctx.__aenter__()
        session = ClientSession(read, write)
        await session.__aenter__()
        await session.initialize()
        set_mcp_session(alias, session)
        app.state.mcp_sessions[alias] = session
    if not app.state.mcp_sessions:
        raise RuntimeError(f"No MCP sessions registered. URIs: {MCP_URIS}")
    print(f"Registered MCP sessions: {list(app.state.mcp_sessions.keys())}")
    try:
        yield
    finally:
        # tear down in reverse
        for session in app.state.mcp_sessions.values():
            await session.__aexit__(None, None, None)
        for ctx in app.state.mcp_contexts[::-1]:
            await ctx.__aexit__(None, None, None)

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.get("/")(lambda: {"message": "welcome to mongo query generator current path is /mqg"})

router = APIRouter(prefix="/mqg")
router.include_router(api_router)
app.include_router(router)

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    print(f"HTTPException: {exc}")
    return JSONResponse(status_code=400, content={"detail": "Bad Request"})

@app.get("/mcp/health")
def mcp_health():
    return {"aliases": list(app.state.mcp_sessions.keys())}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=True)

