from fastapi import APIRouter, Depends, Request
from typing import Optional, Dict, Any
from config.database import get_db_aliases, get_db
from typing import Dict, Any
from config.database import get_db_aliases, get_db
from langgraph_agent.graph import build_graph
from mcp_server.models.model import QueryRequest, AggregateRequest, FindRequest, GraphQueryRequest
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/query")


# Dependency to get MCP server from app state
def get_mcp_server(request: Request):
    return request.app.state.mcp

@router.get("/databases")
async def list_databases():
    """List configured DB aliases and their mapped database names."""
    aliases = get_db_aliases()
    mapping = {alias: get_db(alias).name for alias in aliases}
    return {"aliases": aliases, "mapping": mapping}

@router.get("/all_collections_list")
async def get_collections(db: Optional[str] = "Inventory", mcp = Depends(get_mcp_server)):
    """Get list of all collections in the database."""
    result = await mcp.get_all_collection_list(db=db)
    return result

@router.get("/schema/{collection_name}")
async def get_collection_schema(collection_name: str, db: Optional[str] = "Inventory", mcp = Depends(get_mcp_server)):
    """Get schema information for a specific collection."""
    result = await mcp.get_schema(collection_name, db=db)
    return result

@router.post("/execute")
async def execute_query(request: QueryRequest, mcp = Depends(get_mcp_server)):
    """Execute a MongoDB query."""
    result = await mcp.execute_mongo_query(
        db=request.db,
        collection=request.collection,
        filters=request.filters,
        projection=request.projection,
        sort=request.sort,
        limit=request.limit
    )
    return result

@router.post("/find")
async def find_documents(request: FindRequest, mcp = Depends(get_mcp_server)):
    """Find documents with optional filtering and pagination."""
    result = await mcp.find(
        db=request.db,
        collection=request.collection,
        filters=request.filters,
        projection=request.projection,
        sort=request.sort,
        skip=request.skip,
        limit=request.limit
    )
    return result

@router.post("/aggregate")
async def aggregate_documents(request: AggregateRequest, mcp = Depends(get_mcp_server)):
    """Execute an aggregation pipeline."""
    result = await mcp.aggregate(
        db=request.db,
        collection=request.collection,
        pipeline=request.pipeline
    )
    return result

@router.get("/count/{collection}")
async def count_documents(collection: str, filters: Optional[Dict[str, Any]] = None, db: Optional[str] = "Inventory", mcp = Depends(get_mcp_server)):
    """Count documents in a collection."""
    result = await mcp.count_documents(collection=collection, filters=filters, db=db)
    return result

@router.get("/distinct/{collection}/{field}")
async def get_distinct_values(collection: str, field: str, filters: Optional[Dict[str, Any]] = None, db: Optional[str] = "Inventory", mcp = Depends(get_mcp_server)):
    """Get distinct values for a field."""
    result = await mcp.distinct_values(collection=collection, field=field, filters=filters, db=db)
    return result

@router.get("/sample/{collection_name}")
async def sample_collection(collection_name: str, size: int = 5, db: Optional[str] = "Inventory", mcp = Depends(get_mcp_server)):
    """Get a random sample of documents from a collection."""
    result = await mcp.sample_collection(collection_name=collection_name, size=size, db=db)
    return result

@router.post("/validate")
async def validate_query(request: QueryRequest, mcp = Depends(get_mcp_server)):
    """Validate a MongoDB query."""
    result = await mcp.validate_query(
        db=request.db,
        collection=request.collection,
        filters=request.filters
    )
    return result

@router.get("/db-info")
async def get_database_info(db: Optional[str] = "Inventory", mcp = Depends(get_mcp_server)):
    """Get database information."""
    result = await mcp.get_db_info(db=db)
    return result


@router.post("/graph_query")
async def process_graph_query(request: GraphQueryRequest):
    try:
        print(f"Processing query: {request.query}")
        graph = build_graph()
        ret = await graph.ainvoke({"input": request.query}, return_state=True)
        final_result = ret.get("result") or ret.get("query") or ret if isinstance(ret, dict) else ret
        print(f"Final processed result: {final_result}")
        return JSONResponse(content=jsonable_encoder({"input": request.query, "result": final_result}))
    except Exception as e:
        print(f"Graph execution failed: {e}")
        return JSONResponse(status_code=500, content=jsonable_encoder({"input": request.query, "error": str(e)}))