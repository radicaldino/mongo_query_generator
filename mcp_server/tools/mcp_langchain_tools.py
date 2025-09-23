# langgraph_agent/nodes/mcp_langchain_tools.py
import collections
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from langchain.tools import StructuredTool
from mcp_server.main import get_mcp_session
from config.database import db_name_for_alias
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from config.settings import settings

import json
import re

import logging, os
from datetime import datetime, timezone

log_flow = logging.getLogger("flow")
log_llm = logging.getLogger("llm")
log_mcp = logging.getLogger("mcp")
logger = logging.getLogger("app")


async def _as_json(result: Any) -> Any:
    print(f"DEBUG: _as_json called with type: {type(result)}")
    
    parts = getattr(result, "content", []) or []
    print(f"DEBUG: Parts from content: {len(parts)} parts")
    
    for i, part in enumerate(parts):
        print(f"DEBUG: Processing part {i} type: {type(part)}")
        
        # Handle TextContent objects
        if hasattr(part, 'text'):
            text = part.text.strip()
            print(f"DEBUG: Part {i} text length: {len(text)}")
            
            # Look for the untrusted-user-data section which contains the collections
            if '<untrusted-user-data-' in text and '</untrusted-user-data-' in text:
                print(f"DEBUG: Found untrusted-user-data section in part {i}")
                
                # Find the start and end positions of the untrusted-user-data section
                start_tag = '<untrusted-user-data-'
                end_tag = '</untrusted-user-data-'
                
                # Find the SECOND occurrence of the start tag (skip the first one in the warning)
                start_pos = text.find(start_tag)
                if start_pos != -1:
                    start_pos = text.find(start_tag, start_pos + 1)  # Find second occurrence
                
                # Find the end tag that comes AFTER the second start tag
                end_pos = text.find(end_tag, start_pos) if start_pos != -1 else -1
                
                print(f"DEBUG: First start_pos: {text.find(start_tag)}")
                print(f"DEBUG: Second start_pos: {start_pos}")
                print(f"DEBUG: End pos after start: {end_pos}")
                
                if start_pos != -1 and end_pos != -1:
                    # Find the end of the opening tag
                    tag_end = text.find('>', start_pos)
                    if tag_end != -1:
                        # Extract content between the tags
                        collection_text = text[tag_end + 1:end_pos].strip()
                        print(f"DEBUG: Extracted collection text length: {len(collection_text)}")
                        
                        # Check if this looks like JSON (starts with { or [)
                        if collection_text.startswith('{') or collection_text.startswith('['):
                            try:
                                parsed = json.loads(collection_text)
                                print(f"DEBUG: Parsed JSON from untrusted section: {type(parsed)}")
                                return parsed
                            except Exception as e:
                                print(f"DEBUG: Failed to parse JSON from untrusted section: {e}")
                                # Fall through to collection name parsing
                        
                        # Parse the collection names (they're quoted strings, one per line)
                        collections = []
                        for line in collection_text.split('\n'):
                            line = line.strip()
                            if line and line.startswith('"') and line.endswith('"'):
                                collection_name = line[1:-1]  # Remove quotes
                                collections.append(collection_name)
                        
                        print(f"DEBUG: Parsed {len(collections)}...")
                        return collections
                    else:
                        print(f"DEBUG: Could not find end of opening tag")
                else:
                    print(f"DEBUG: Could not find start or end tags")
            
            # Try to parse as JSON if it looks like JSON
            if text.startswith('{') or text.startswith('['):
                try:
                    parsed = json.loads(text)
                    print(f"DEBUG: Parsed JSON from part {i}: {parsed}")
                    return parsed
                except Exception as e:
                    print(f"DEBUG: Failed to parse JSON from part {i}: {e}")
                    continue
    
    print(f"DEBUG: No valid data found, returning None")
    return None

def _normalize_date_str(s: str, end=False) -> str:
    # Start of day or end of day Zulu
    if end:
        return f"{s}T23:59:59Z"
    return f"{s}T00:00:00Z"

def _normalize_filter_dates(f: dict) -> dict:
    # shallow normalization for $gte/$lte common pattern
    f = dict(f or {})
    for k, v in list(f.items()):
        if isinstance(v, dict):
            if "$gte" in v and isinstance(v["$gte"], str) and len(v["$gte"]) == 10:
                v["$gte"] = _normalize_date_str(v["$gte"], end=False)
            if "$lte" in v and isinstance(v["$lte"], str) and len(v["$lte"]) == 10:
                v["$lte"] = _normalize_date_str(v["$lte"], end=True)
    return f

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

async def _list_collections(alias: str, database: Optional[str] = None):
    session = get_mcp_session(alias)
    db = database or db_name_for_alias(alias)
    log_mcp.info("MCP_CALL tool=list-collections alias=%s db=%s", alias, db)
    res = await session.call_tool("list-collections", {"database": db})
    data = await _as_json(res)
    log_mcp.info("MCP_RESULT tool=list-collections size=%s preview=%s", (len(data) if isinstance(data, list) else 'n/a'), str(data)[:400])
    if isinstance(data, list):
        return data
    return (data or {}).get("collections") or (data or {}).get("result") or []

async def _collection_schema(alias: str, collection: str, database: Optional[str] = None):
    session = get_mcp_session(alias)
    db = database or db_name_for_alias(alias)
    log_mcp.info("MCP_CALL tool=collection-schema alias=%s db=%s collection=%s", alias, db, collection)
    res = await session.call_tool("collection-schema", {"database": db, "collection": collection})
    data = await _as_json(res)
    log_mcp.info("MCP_RESULT tool=collection-schema preview=%s", str(data)[:400])
    return data or {}

async def _find(alias: str, collection: str, database: Optional[str] = None,
                filter: Optional[Dict[str, Any]] = None,
                projection: Optional[Dict[str, int]] = None,
                sort: Optional[Dict[str, int]] = None,
                limit: int = 100):
    session = get_mcp_session(alias)
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
    res = await session.call_tool("find", payload)
    data = await _as_json(res)
    # If server returned only a text message (e.g., "Found 0 documents"), data will be None.
    if data is None:
        return {"documents": []}
    if isinstance(data, list):
        return {"documents": data}
    return data or {}

async def _aggregate(alias: str, collection: str, pipeline: List[Dict[str, Any]], database: Optional[str] = None):
    session = get_mcp_session(alias)
    db = database or db_name_for_alias(alias)
    log_mcp.info("MCP_CALL tool=aggregate alias=%s db=%s collection=%s pipeline_len=%s", alias, db, collection, len(pipeline or []))
    res = await session.call_tool("aggregate", {"database": db, "collection": collection, "pipeline": pipeline})
    data = await _as_json(res)
    log_mcp.info("MCP_RESULT tool=aggregate preview=%s", str(data)[:400])
    if isinstance(data, list):
        return {"documents": data}
    return data or {}

def build_mcp_langchain_tools() -> List[StructuredTool]:
    return [
        StructuredTool.from_function(
            name="list_collections",
            description="List collections in a database (via MCP).",
            args_schema=ListCollectionsArgs,
            coroutine=lambda alias, database=None: _list_collections(alias, database),
        ),
        StructuredTool.from_function(
            name="collection_schema",
            description="Get collection schema (via MCP).",
            args_schema=GetSchemaArgs,
            coroutine=lambda alias, collection, database=None: _collection_schema(alias, collection, database),
        ),
        StructuredTool.from_function(
            name="find_documents",
            description="Find documents (via MCP).",
            args_schema=FindArgs,
            coroutine=lambda alias, collection, database=None, filter=None, projection=None, sort=None, limit=100:
                _find(alias, collection, database, filter, projection, sort, limit),
        ),
        StructuredTool.from_function(
            name="aggregate_documents",
            description="Run aggregation pipeline (via MCP).",
            args_schema=AggregateArgs,
            coroutine=lambda alias, collection, pipeline, database=None:
                _aggregate(alias, collection, pipeline, database),
        ),
    ]

# LLM Tool Calling Helper
llm = ChatOpenAI(api_key=settings.OPENAI_API_KEY, model=settings.OPENAI_MODEL, temperature=0)

async def llm_with_mcp_tools(system_text: str, user_text: str, max_steps: int = 2):
    tools = build_mcp_langchain_tools()
    llm_with = llm.bind_tools(tools)
    messages = [HumanMessage(content=f"{system_text}\n\n{user_text}")]

    log_llm.info("LLM START system_len=%s user_len=%s tools=%s", len(system_text), len(user_text), [t.name for t in tools])
    ai = await llm_with.ainvoke(messages)
    log_llm.info("LLM TURN content_len=%s tool_calls=%s", len(ai.content or ""), getattr(ai, "tool_calls", None))

    steps = 0
    while isinstance(ai, AIMessage) and ai.tool_calls and steps < max_steps:
        tool_msgs = []
        for call in ai.tool_calls:
            name = call["name"]; args = call.get("args", {}) or {}; call_id = call["id"]

            # Try to rescue a pipeline from the AI message content if missing
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

            # Enforce presence of pipeline after rescue; prompt the model to retry if still missing
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