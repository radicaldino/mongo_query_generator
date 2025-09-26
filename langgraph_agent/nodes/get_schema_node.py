from langgraph_agent.state import AgentState
import asyncio, json
from mcp_server.tools.mcp_langchain_tools import llm_with_mcp_tools, _collection_schema
import logging
from langsmith import traceable
import anyio


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
    all_empty = all(not (s or {}) for s in raw_schemas.values())
    if all_empty:
        probe_errors = []
        try:
            # Re-run schema tool calls sequentially to surface any exceptions
            from mcp_server.tools.mcp_langchain_tools import _collection_schema as _probe_schema
            for col in collections:
                try:
                   _= await _probe_schema(alias, col)
                except Exception as e:
                    probe_errors.append(f"{col}: {e}")
        except Exception as e:
        # Unexpected failure while probing
            raise RuntimeError(f"MCP probing failed: {e}")

        if probe_errors:
            # End the graph with the concrete MCP error(s)
            raise RuntimeError(f"MCP collection-schema failed: {'; '.join(probe_errors)}")

    # No concrete tool errors, but schemas are empty -> redirect back to choose-collection
        return {
            "schema": {},
            "error": f"Empty schemas returned for collections {collections}",
            "redirect_to_choose": True,
            "feedback": "Schemas were empty for the selected collections. Please reselect different collections."
        }
    print(f"Raw schemas: {raw_schemas}")

     # 2) Let LLM infer relations and request any extra live checks via MCP tools
    system = (
    "You are part of a MongoDB query generator. "
    "Your role is to analyze user intent and select the most relevant schemas "
    "from the provided collections. "
    "You must also infer relationships between collections, enrich field details, "
    "and produce guidance for query construction. "
    "\n\n"
    "You can call tools in real time to confirm schema knowledge: "
    "- Use collection_schema(alias, collection) to inspect fields and types. "
    "- Use find_documents(alias, collection, filter, limit) to confirm field presence, formats, and example values. "
    "\n\n"
    "When producing output: "
    "- Return JSON ONLY in the required schema. "
    "- Do not invent fields or collections. "
    "- Only include fields validated by tools or visible in provided schemas. "
    "- Ensure relations and notes are concrete, not vague guesses."
)


    user = (
    f"User query: {state.get('input','')}\n"
    f"Parsed intent: {state.get('parsed','(no parsed intent available)')}\n"
    f"Candidate collections: {collections}\n"
    f"Pre-fetched schemas (may be partial): {json.dumps(raw_schemas, default=str)[:4000]}\n"
    "\n"
    "Tasks for you:\n"
    "1. Understand the user’s intent from query + parsed intent.\n"
    "2. Inspect the provided schemas and identify which collections are most relevant.\n"
    "3. Infer relationships between collections by checking *_id, <name>_id, and ObjectId references.\n"
    "   - Confirm with samples if uncertain.\n"
    "4. Enrich each selected schema with: field types, examples, nullable flags, and category fields "
    "(keys, locations, company, products, quantities, dates, enums, booleans).\n"
    "5. Produce concrete notes to guide query building:\n"
    "   - primary_candidates: best starting collections for queries\n"
    "   - recommended_joins: useful joins with direction\n"
    "   - natural_filters: conditions that make sense (company_id, dates, status)\n"
    "   - useful_sorts: likely sort orders (e.g. -createdAt, quantity desc)\n"
    "   - aggregation_hints: group/lookup suggestions\n"
    "   - ambiguities: unclear/multiple-field overlaps\n"
    "   - missing_info: uncertain types or needed samples\n"
    "   - field_aliases: normalize naming (e.g. companyID → company_id)\n"
    "   - id_types: clarify id field types (number, ObjectId, string)\n"
    "   - report_defaults: useful defaults for projections, filters, and location behavior\n"
    "\n"
    "Output must be strictly valid JSON per this schema:\n"
    "{\n"
    "  \"schema\": {\n"
    "    \"<collection>\": {\n"
    "      \"fields\": {\"<field>\": {\"type\": \"<type|unknown>\", \"example\": \"<sample|None>\", \"nullable\": \"<true|false>\"}},\n"
    "      \"key_fields\": [\"_id\", ...],\n"
    "      \"location_fields\": [...],\n"
    "      \"company_fields\": [...],\n"
    "      \"product_fields\": [...],\n"
    "      \"quantity_fields\": [...],\n"
    "      \"date_fields\": [...],\n"
    "      \"enum_fields\": {\"status\": [\"Pending\", \"Done\"]},\n"
    "      \"boolean_fields\": [...],\n"
    "      \"counts\": {\"sampled\": <int>}\n"
    "    }\n"
    "  },\n"
    "  \"relations\": [\n"
    "    {\"from\": \"<collection>\", \"field\": \"<local_field>\", \"to\": \"<collection>\", \"foreign_field\": \"<remote_field>\", \"join_type\": \"one-to-one|one-to-many|many-to-one|unknown\", \"confidence\": 0.0}\n"
    "  ],\n"
    "  \"notes\": {\n"
    "    \"primary_candidates\": [\"<collection>\", ...],\n"
    "    \"recommended_joins\": [\"A.localID -> B._id\", ...],\n"
    "    \"natural_filters\": [\"date range on <field>\", \"company_id equals ...\"],\n"
    "    \"useful_sorts\": [\"-createdAt\", \"quantity desc\"],\n"
    "    \"aggregation_hints\": [\"group by <field>\", \"lookup <collection> on <field>\"],\n"
    "    \"ambiguities\": [\"<field> appears in multiple collections; disambiguate by ...\"],\n"
    "    \"missing_info\": [\"<field> type unclear; sample suggests string ISO date\"],\n"
    "    \"field_aliases\": {\"companyID\": \"company_id\"},\n"
    "    \"id_types\": {\"company_id\": \"number\", \"material_id\": \"number\"},\n"
    "    \"report_defaults\": {\"projection\": [\"material_name\", \"material_id\", \"available_quantity\"], \"filters\": {\"isActive\": true, \"activeListing\": true}, \"location_behavior\": \"If no location specified, use `default_location_id`.\"}\n"
    "  }\n"
    "}"
            )


    
    with anyio.CancelScope() as scope:
        ai = await llm_with_mcp_tools(system, user, max_steps=3)
        if scope.cancel_called:
            raise Exception(str({"error": "cancelled"}))
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

   # Prefer LLM-merged schemas; keep the raw as reference
    normalized = {}
    for col in collections:
        raw = raw_schemas.get(col, {}) or {}
        merged = (schemas or {}).get(col, {}) or {}
        fields = (merged.get("fields") if isinstance(merged, dict) else None) or (merged if isinstance(merged, dict) else raw)
        field_names = list(fields.keys()) if isinstance(fields, dict) else []
        normalized[col] = {
        "schema": fields,         # enriched/canonical
        "raw": raw,               # original from MCP
        "total_fields": len(field_names),
        "field_names": field_names,
        "key_fields": merged.get("key_fields", []),
        "date_fields": merged.get("date_fields", []),
        "enum_fields": merged.get("enum_fields", {}),
        "counts": merged.get("counts", {}),
    }
    else:
        normalized[col] = {
            "schema": {},
            "total_fields": 0,
            "field_names": [],
            }

    #this is being passed to the build_query_node.
    schema_catalog = {
        "collections": normalized,
        "total_collections": len(normalized),
        "database": alias,
        "relations": relations,
        "notes": notes,
    }
    print("=== END GET SCHEMA NODE ===\n")
    log_flow.info("NODE_END name=get_schema schema=%s", schema_catalog)
    # clear any stale redirects on success
    result = {"schemas": schema_catalog}
    result["redirect_to_choose"] = False
    result["feedback"] = ""
    return result

