from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from langchain.tools import StructuredTool
from config.database import db_name_for_alias
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from config.settings import settings
from config.database import db_name_for_alias
from mcp_server.session_manager import RequestScopedSessionManager
import anyio
import json
import logging
import time

log_flow = logging.getLogger("flow")
log_llm = logging.getLogger("llm")
log_mcp = logging.getLogger("mcp")
logger = logging.getLogger("app")

async def call_tool_with_timeout(mgr: RequestScopedSessionManager, alias: str, tool_name: str, payload: dict, timeout_s: float = 20.0):
    """Call MCP tool with timeout and proper error handling"""
    try:
        session = mgr.get(alias)
        with anyio.move_on_after(timeout_s) as scope:
            res = await session.call_tool(tool_name, payload)
        if scope.cancel_called:
            raise TimeoutError(f"MCP tool '{tool_name}' timed out after {timeout_s}s for alias '{alias}'")
        return res
    except Exception as e:
        logger.warning(f"MCP call failed for {alias}, attempting reconnect: {e}")
        session = await mgr.reconnect(alias)
        with anyio.move_on_after(timeout_s) as scope:
            res = await session.call_tool(tool_name, payload)
        if scope.cancel_called:
            raise TimeoutError(f"MCP tool '{tool_name}' timed out after {timeout_s}s for alias '{alias}' (post-reconnect)")
        return res

async def _as_json(result: Any) -> Any:
    """Parse MCP result to JSON"""
    parts = getattr(result, "content", []) or []
    
    for part in parts:
        if hasattr(part, 'text'):
            text = part.text.strip()
            
            # Handle untrusted-user-data sections
            if '<untrusted-user-data-' in text and '</untrusted-user-data-' in text:
                start_tag = '<untrusted-user-data-'
                end_tag = '</untrusted-user-data-'
                
                start_pos = text.find(start_tag)
                if start_pos != -1:
                    start_pos = text.find(start_tag, start_pos + 1)  # Find second occurrence
                
                end_pos = text.find(end_tag, start_pos) if start_pos != -1 else -1
                
                if start_pos != -1 and end_pos != -1:
                    tag_end = text.find('>', start_pos)
                    if tag_end != -1:
                        collection_text = text[tag_end + 1:end_pos].strip()
                        
                        if collection_text.startswith('{') or collection_text.startswith('['):
                            try:
                                return json.loads(collection_text)
                            except Exception:
                                pass
                        
                        # Parse collection names
                        collections = []
                        for line in collection_text.split('\n'):
                            line = line.strip()
                            if line and line.startswith('"') and line.endswith('"'):
                                collection_name = line[1:-1]
                                collections.append(collection_name)
                        return collections
            
            # Try to parse as JSON
            if text.startswith('{') or text.startswith('['):
                try:
                    return json.loads(text)
                except Exception:
                    continue
    
    return None

def _normalize_date_str(s: str, end=False) -> str:
    if end:
        return f"{s}T23:59:59Z"
    return f"{s}T00:00:00Z"

def _normalize_filter_dates(f: dict) -> dict:
    f = dict(f or {})
    for k, v in list(f.items()):
        if isinstance(v, dict):
            if "$gte" in v and isinstance(v["$gte"], str) and len(v["$gte"]) == 10:
                v["$gte"] = _normalize_date_str(v["$gte"], end=False)
            if "$lte" in v and isinstance(v["$lte"], str) and len(v["$lte"]) == 10:
                v["$lte"] = _normalize_date_str(v["$lte"], end=True)
    return f

# Tool argument schemas
class ListCollectionsArgs(BaseModel):
    alias: str = Field(..., description="Alias name (e.g., Inventory)")
    database: Optional[str] = Field(None, description="Override DB name; default inferred from alias")

class GetSchemaArgs(BaseModel):
    alias: str
    collection: str
    database: Optional[str] = None

class FindArgs(BaseModel):
    alias: str
    collection: str
    database: Optional[str] = None
    filter: Optional[Dict[str, Any]] = None
    projection: Optional[Dict[str, int]] = None
    sort: Optional[Dict[str, int]] = None
    limit: Optional[int] = 100

class AggregateArgs(BaseModel):
    alias: str
    collection: str
    pipeline: List[Dict[str, Any]] = Field(default_factory=list)
    database: Optional[str] = None




# Tool implementations
async def _list_collections(mgr: RequestScopedSessionManager, alias: str, database: Optional[str] = None):
    if not mgr:
        raise RuntimeError("MCP session manager not provided")
    db = database or db_name_for_alias(alias)
    log_mcp.info("MCP_CALL tool=list-collections alias=%s db=%s", alias, db)
    res = await call_tool_with_timeout(mgr, alias, "list-collections", {"database": db}, timeout_s=20.0)
    data = await _as_json(res)
    log_mcp.info("MCP_RESULT tool=list-collections size=%s preview=%s", (len(data) if isinstance(data, list) else 'n/a'), str(data)[:400])
    if isinstance(data, list):
        return data
    return (data or {}).get("collections") or (data or {}).get("result") or []

async def _collection_schema(mgr: RequestScopedSessionManager, alias: str, collection: str, database: Optional[str] = None):
    if not mgr:
        raise RuntimeError("MCP session manager not provided")
    db = database or db_name_for_alias(alias)
    log_mcp.info("MCP_CALL tool=collection-schema alias=%s db=%s collection=%s", alias, db, collection)
    res = await call_tool_with_timeout(mgr, alias, "collection-schema", {"database": db, "collection": collection}, timeout_s=25.0)
    data = await _as_json(res)
    log_mcp.info("MCP_RESULT tool=collection-schema preview=%s", str(data)[:400])
    return data or {}

async def _find(mgr: RequestScopedSessionManager, alias: str, collection: str, database: Optional[str] = None,
                filter: Optional[Dict[str, Any]] = None,
                projection: Optional[Dict[str, int]] = None,
                sort: Optional[Dict[str, int]] = None,
                limit: int = 100):
    if not mgr:
        raise RuntimeError("MCP session manager not provided")
    db = database or db_name_for_alias(alias)
    norm_filter = _normalize_filter_dates(filter or {})
    payload = {"database": db, "collection": collection, "filter": norm_filter, "limit": limit}
    if projection is not None:
        payload["projection"] = projection
    if sort is not None:
        payload["sort"] = sort

    log_mcp.info("MCP_CALL tool=find alias=%s db=%s collection=%s filter=%s limit=%s proj=%s sort=%s",
                 alias, db, collection, payload["filter"], limit,
                 ("set" if "projection" in payload else "omitted"),
                 ("set" if "sort" in payload else "omitted"))

    res = await call_tool_with_timeout(mgr, alias, "find", payload, timeout_s=25.0)
    data = await _as_json(res)

    if data is None:
        return {"documents": []}
    if isinstance(data, list):
        return {"documents": data}
    return data or {}

async def _aggregate(mgr: RequestScopedSessionManager, alias: str, collection: str, pipeline: List[Dict[str, Any]], database: Optional[str] = None):
    if not mgr:
        raise RuntimeError("MCP session manager not provided")
    db = database or db_name_for_alias(alias)
    log_mcp.info("MCP_CALL tool=aggregate alias=%s db=%s collection=%s pipeline_len=%s", alias, db, collection, len(pipeline or []))
    res = await call_tool_with_timeout(mgr, alias, "aggregate", {"database": db, "collection": collection, "pipeline": pipeline}, timeout_s=30.0)
    data = await _as_json(res)
    log_mcp.info("MCP_RESULT tool=aggregate preview=%s", str(data)[:400])
    if isinstance(data, list):
        return {"documents": data}
    return data or {}

async def _aggregate_with_debug(mgr: RequestScopedSessionManager, alias: str, collection: str, pipeline: list, database: Optional[str] = None):
    """Run aggregate stage-by-stage to detect where results drop to zero; returns plain dict with debug info."""
    if not mgr:
        raise RuntimeError("MCP session manager not provided")
    session = mgr.get(alias)

    stages_info = []
    last_docs = []
    started = time.time()
    empty_after_stage = None
    last_working_stage = None
    error_msgs = []

    for i, stage in enumerate(pipeline or []):
        partial = pipeline[: i + 1]
        stage_start = time.time()
        try:
            res = await call_tool_with_timeout(
                mgr,
                alias,
                "aggregate",
                {"database": (database or db_name_for_alias(alias)), "collection": collection, "pipeline": partial},
                timeout_s=30.0,
            )
            stage_time_ms = (time.time() - stage_start) * 1000.0
            data = await _as_json(res)
            if isinstance(data, list):
                docs = data
            elif isinstance(data, dict):
                docs = data.get("documents") or data.get("data") or []
            else:
                docs = []
            count = len(docs)

            stages_info.append({
                "stage_index": i,
                "stage": stage,
                "documents_after_stage": count,
                "execution_time_ms": stage_time_ms,
                "success": True,
            })

            last_docs = docs
            if count == 0 and empty_after_stage is None:
                empty_after_stage = i
                last_working_stage = (i - 1) if i > 0 else None
                # We can stop early once it’s empty
                break
        except Exception as e:
            stage_time_ms = (time.time() - stage_start) * 1000.0
            msg = f"Stage {i} error: {e}"
            error_msgs.append(msg)
            stages_info.append({
                "stage_index": i,
                "stage": stage,
                "documents_after_stage": 0,
                "execution_time_ms": stage_time_ms,
                "success": False,
                "error": str(e),
            })
            # stop on first failure
            break

    total_time_ms = (time.time() - started) * 1000.0
    final_count = len(last_docs) if isinstance(last_docs, list) else 0
    success = len(error_msgs) == 0

    debug_info = {
        "collection": collection,
        "pipeline_length": len(pipeline or []),
        "stages": stages_info,
        "final_count": final_count,
        "execution_time_ms": total_time_ms,
        "errors": error_msgs,
        "success": success,
        "empty_after_stage": empty_after_stage,
        "last_working_stage": last_working_stage,
    }

    return {
        "documents": last_docs if isinstance(last_docs, list) else [],
        "debug_info": debug_info,
        "success": success,
    }


async def _find_with_debug(mgr: RequestScopedSessionManager, alias: str, collection: str, database: Optional[str] = None,
                           filter: Optional[Dict[str, Any]] = None,
                           projection: Optional[Dict[str, int]] = None,
                           sort: Optional[Dict[str, int]] = None,
                           limit: int = 100):
    """Run find with timing and basic debug info; returns plain dict with debug info."""
    if not mgr:
        raise RuntimeError("MCP session manager not provided")
    session = mgr.get(alias)

    started = time.time()
    try:
        res = await call_tool_with_timeout(
            mgr,
            alias,
            "find",
            {
                "database": (database or db_name_for_alias(alias)),
                "collection": collection,
                "filter": (filter or {}),
                "projection": projection,
                "sort": sort,
                "limit": limit,
            },
            timeout_s=25.0,
        )
        total_time_ms = (time.time() - started) * 1000.0
        docs = _extract_documents(res)
        doc_count = len(docs) if isinstance(docs, list) else 0

        debug_info = {
            "collection": collection,
            "filter": (filter or {}),
            "projection": projection,
            "sort": sort,
            "limit": limit,
            "execution_time_ms": total_time_ms,
            "document_count": doc_count,
            "success": True,
        }

        return {
            "documents": docs if isinstance(docs, list) else [],
            "debug_info": debug_info,
            "success": True,
        }
    except Exception as e:
        total_time_ms = (time.time() - started) * 1000.0
        debug_info = {
            "collection": collection,
            "filter": (filter or {}),
            "projection": projection,
            "sort": sort,
            "limit": limit,
            "execution_time_ms": total_time_ms,
            "error": str(e),
            "success": False,
        }
        return {
            "documents": [],
            "debug_info": debug_info,
            "success": False,
        }

def _extract_documents(result):
    """Extract documents from MCP result"""
    if not result:
        return []
    
    # Handle different MCP result formats
    if hasattr(result, 'content'):
        for part in result.content or []:
            if hasattr(part, 'text'):
                try:
                    data = json.loads(part.text)
                    if isinstance(data, dict):
                        return data.get('documents', data.get('data', []))
                    elif isinstance(data, list):
                        return data
                except:
                    pass
    
    return []

def build_request_mcp_tools(mgr: RequestScopedSessionManager) -> List[StructuredTool]:
    """Build MCP tools with request-scoped session manager"""
    return [
        StructuredTool.from_function(
            name="list_collections",
            description="List collections in a database (via MCP).",
            args_schema=ListCollectionsArgs,
            coroutine=lambda alias, database=None: _list_collections(mgr, alias, database),
        ),
        StructuredTool.from_function(
            name="collection_schema",
            description="Get collection schema (via MCP).",
            args_schema=GetSchemaArgs,
            coroutine=lambda alias, collection, database=None: _collection_schema(mgr, alias, collection, database),
        ),
        StructuredTool.from_function(
            name="find_documents",
            description="Find documents (via MCP).",
            args_schema=FindArgs,
            coroutine=lambda alias, collection, database=None, filter=None, projection=None, sort=None, limit=100:
                _find(mgr, alias, collection, database, filter, projection, sort, limit),
        ),
        StructuredTool.from_function(
            name="aggregate_documents",
            description="Run aggregation pipeline (via MCP).",
            args_schema=AggregateArgs,
            coroutine=lambda alias, collection, pipeline, database=None:
                _aggregate(mgr, alias, collection, pipeline, database),
        ),
    ]

# LLM Tool Calling Helper
llm = ChatOpenAI(api_key=settings.OPENAI_API_KEY, model=settings.OPENAI_MODEL, temperature=0)

async def llm_with_mcp_tools(mgr: RequestScopedSessionManager, system_text: str, user_text: str, max_steps: int = 2):
    """LLM with request-scoped MCP tools"""
    tools = build_request_mcp_tools(mgr)
    llm_with = llm.bind_tools(tools)
    messages = [HumanMessage(content=f"{system_text}\n\n{user_text}")]

    log_llm.info("LLM START system_len=%s user_len=%s tools=%s", len(system_text), len(user_text), [t.name for t in tools])
    ai = await llm_with.ainvoke(messages)
    log_llm.info("LLM TURN content_len=%s tool_calls=%s", len(ai.content or ""), getattr(ai, "tool_calls", None))

    steps = 0
    while isinstance(ai, AIMessage) and ai.tool_calls and steps < max_steps:
        tool_msgs = []
        for call in ai.tool_calls:
            name = call["name"]
            args = call.get("args", {}) or {}
            call_id = call["id"]

            # Handle aggregate_documents pipeline rescue
            if name == "aggregate_documents" and not args.get("pipeline"):
                try:
                    txt = (ai.content or "").strip()
                    if txt.startswith("```json"): txt = txt[7:]
                    if txt.endswith("```"): txt = txt[:-3]
                    txt = txt.strip()
                    import re, json as _json
                    m = re.search(r'\{[\s\S]*\}', txt)
                    if m:
                        js = _json.loads(m.group(0))
                        if isinstance(js, dict) and isinstance(js.get("pipeline"), list):
                            args["pipeline"] = js["pipeline"]
                except Exception:
                    pass

            # Enforce pipeline presence
            if name == "aggregate_documents" and (not isinstance(args.get("pipeline"), list) or not args["pipeline"]):
                tool_msgs.append(
                    ToolMessage(
                        content="Error: aggregate_documents requires a non-empty 'pipeline' array (use stages like $match, $group, $project, $sort). Include it and retry.",
                        tool_call_id=call_id,
                    )
                )
                continue

            log_llm.info("TOOL_CALL name=%s args=%s", name, args)
            tool = next((t for t in tools if t.name == name), None)
            if not tool:
                log_llm.warning("TOOL_MISSING name=%s", name)
                tool_msgs.append(ToolMessage(content=f"Tool {name} not found", tool_call_id=call_id))
                continue
            try:
                result = await tool.ainvoke(args)
                log_llm.info("TOOL_RESULT name=%s preview=%s", name, str(result)[:400])
                tool_msgs.append(ToolMessage(content=json.dumps(result), tool_call_id=call_id))
            except Exception as e:
                log_llm.exception("TOOL_ERROR name=%s error=%s", name, e)
                tool_msgs.append(ToolMessage(content=f"Error: {e}", tool_call_id=call_id))
        messages.extend([ai, *tool_msgs])
        ai = await llm_with.ainvoke(messages)
        log_llm.info("LLM TURN content_len=%s tool_calls=%s", len(ai.content or ""), getattr(ai, "tool_calls", None))
        steps += 1

    log_llm.info("LLM FINAL content_preview=%s", (ai.content or "")[:400])
    return ai