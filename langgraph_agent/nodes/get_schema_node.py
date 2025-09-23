from langgraph_agent.state import AgentState
import asyncio, json
from mcp_server.tools.mcp_langchain_tools import llm_with_mcp_tools, _collection_schema
import logging
import os
from langsmith import traceable


log_flow = logging.getLogger("flow")
logger = logging.getLogger("app")
log_llm = logging.getLogger("llm")
log_mcp = logging.getLogger("mcp")

@traceable(name="get_schema_node")
async def get_schema_node(state: AgentState) -> AgentState:
    log_flow.info("NODE_START name=get_schema db=%s collections=%s", state.get("db"), state.get("collections"))
    print(f"\n=== GET SCHEMA NODE ===")
    collections = state.get("collections", [])
    if not collections:
        log_flow.info("NODE_END name=get_schema error=No collections provided")
        return {"schema": {}, "error": "No collections provided"}

    alias = state["db"]

    # 1) Prefetch schemas concurrently
    async def fetch(col):
        try:
            return col, await _collection_schema(alias, col)
        except Exception as e:
            return col, {"error": str(e)}
    results = await asyncio.gather(*[fetch(c) for c in collections])
    raw_schemas = {c: s for c, s in results}

     # 2) Let LLM infer relations and request any extra live checks via MCP tools
    system = (
        f"You can analyze pre-fetched collection schemas and call tools to verify relations in real time "
        f"for alias '{alias}'. Use collection_schema if you need more schema detail; use find_documents to sample fields. "
        f"Return JSON only: "
        f'{{"schemas": {{"<collection>": {{...}}}}, "relations": [{{"from": "...","field": "...","to": "...","confidence": 0-1}}], '
        f'"notes": "..."}}'
    )
    user = (
        f"User query: {state.get('input','')}\n"
        f"Collections: {collections}\n"
        f"Pre-fetched schemas (may be partial): {json.dumps(raw_schemas, default=str)[:4000]}\n"
        f"Tasks:\n"
        f"- Infer likely relations across these collections (match *_id, *ID, company_id, material_id, etc.).\n"
        f"- If uncertain, call tools to fetch more evidence (e.g., sample docs, confirm field presence/types).\n"
        f"- Output the merged schemas (use pre-fetched unless you discover better), detected relations.\n"
    )

    ai = await llm_with_mcp_tools(system, user, max_steps=2)
    content = (ai.content or "").strip()
    try:
        if content.startswith("```json"): content = content[7:]
        if content.endswith("```"): content = content[:-3]
        data = json.loads(content)
        schemas = data.get("schemas", raw_schemas)
        relations = data.get("relations", [])
        notes = data.get("notes", "")

    except Exception as e:
        log_flow.info("NODE_ERROR name=get_schema error=%s", str(e))
        return {"schema": {}, "error": str(e)}


    # 3) Normalize to build_query_node’s expected structure
    normalized = {}
    for col, fields in raw_schemas.items():
        if isinstance(fields, dict) and fields:
            field_names = list(fields.keys())
            normalized[col] = {
                "schema": fields,
                "total_fields": len(fields),
                "field_names": field_names,
            }
        else:
            normalized[col] = {
                "schema": {},
                "total_fields": 0,
                "field_names": [],
            }

    # 4) Optional single LLM pass to infer relations/notes (keep simple)
    schema_catalog = {
        "collections": normalized,
        "total_collections": len(normalized),
        "database": alias,
        "relations": [],
        "notes": "",
    }
    print("=== END GET SCHEMA NODE ===\n")
    log_flow.info("NODE_END name=get_schema schema=%s", schema_catalog)
    return {"schema": schema_catalog}
