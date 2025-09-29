import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from apis.api import router as api_router
from config.settings import settings 
import os
import logging
from mcp_server.tools.mcp_langchain_tools import _find, _list_collections, _collection_schema, _aggregate
from config.database import _parse_mapping, alias_for_db, db_name_for_alias
from mcp_server.session_manager import create_request_session_manager



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
)


# define your alias → URI (from your env/config)
MCP_URIS = {
    "Inventory": os.getenv("MDB_MCP_CONNECTION_STRING_INVENTORY", ""),
    "App": os.getenv("MDB_MCP_CONNECTION_STRING_APP", ""),
    "Component": os.getenv("MDB_MCP_CONNECTION_STRING_COMPONENT", ""),
    "Fulfillment": os.getenv("MDB_MCP_CONNECTION_STRING_FULFILLMENT", ""),
}
if not any(MCP_URIS.values()):
    MCP_URIS = _parse_mapping(settings.MONGO_URIS or "")


class McpFacade:
    """Facade that uses per-request MCP sessions for REST API endpoints"""
    
    async def get_all_collection_list(self, db: str):
        alias = alias_for_db(db)
        mgr = create_request_session_manager()
        async with mgr.session_context([alias]):
            return await _list_collections(mgr, alias, database=db_name_for_alias(alias))

    async def get_schema(self, collection: str, db: str):
        alias = alias_for_db(db)
        mgr = create_request_session_manager()
        async with mgr.session_context([alias]):
            return await _collection_schema(mgr, alias, collection, database=db_name_for_alias(alias))

    async def find(self, db: str, collection: str, filters=None, projection=None, sort=None, skip: int = 0, limit: int = 100):
        alias = alias_for_db(db)
        mgr = create_request_session_manager()
        async with mgr.session_context([alias]):
            # Note: skip is not supported in current _find wrapper
            return await _find(mgr, alias, collection, database=db_name_for_alias(alias),
                               filter=filters, projection=projection, sort=sort, limit=limit)

    async def aggregate(self, db: str, collection: str, pipeline: list):
        alias = alias_for_db(db)
        mgr = create_request_session_manager()
        async with mgr.session_context([alias]):
            return await _aggregate(mgr, alias, collection, pipeline, database=db_name_for_alias(alias))

    async def execute_mongo_query(self, db: str, collection: str, filters=None, projection=None, sort=None, limit: int = 100):
        return await self.find(db, collection, filters, projection, sort, 0, limit)

    async def count_documents(self, collection: str, filters=None, db: str = "Inventory"):
        alias = alias_for_db(db)
        mgr = create_request_session_manager()
        async with mgr.session_context([alias]):
            result = await _find(mgr, alias, collection, database=db_name_for_alias(alias),
                                 filter=filters, limit=0)
            return {"count": len(result.get("documents", []))}

    async def distinct_values(self, collection: str, field: str, filters=None, db: str = "Inventory"):
        alias = alias_for_db(db)
        mgr = create_request_session_manager()
        async with mgr.session_context([alias]):
            result = await _find(mgr, alias, collection, database=db_name_for_alias(alias),
                                 filter=filters, limit=1000)
            docs = result.get("documents", [])
            distinct = list(set(doc.get(field) for doc in docs if field in doc))
            return {"distinct_values": distinct}

    async def sample_collection(self, collection: str, size: int = 5, db: str = "Inventory"):
        alias = alias_for_db(db)
        mgr = create_request_session_manager()
        async with mgr.session_context([alias]):
            result = await _find(mgr, alias, collection, database=db_name_for_alias(alias), limit=size)
            return result

    async def validate_query(self, db: str, collection: str, filters):
        try:
            await self.find(db, collection, filters, limit=1)
            return {"valid": True, "message": "Query is valid"}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    async def get_db_info(self, db: str = "Inventory"):
        alias = alias_for_db(db)
        mgr = create_request_session_manager()
        async with mgr.session_context([alias]):
            collections = await _list_collections(mgr, alias, database=db_name_for_alias(alias))
            return {
                "database": db,
                "alias": alias,
                "collections": collections,
                "total_collections": len(collections)
            }


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mcp = McpFacade()

    print(f"Available database aliases: {list(MCP_URIS.keys())}")
    try:
        yield
    finally:
         pass
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
    return {"aliases": [k for k, v in MCP_URIS.items() if v]}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=True)

