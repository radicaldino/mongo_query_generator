from langchain_openai import ChatOpenAI
from langgraph_agent.state import AgentState
import json
import re
from config.settings import settings
from datetime import datetime, timedelta
from langsmith import traceable
import anyio

OPENAI_API_KEY = settings.OPENAI_API_KEY
OPENAI_MODEL = settings.OPENAI_MODEL

llm = ChatOpenAI(api_key=OPENAI_API_KEY, model=OPENAI_MODEL, temperature=0.1)

@traceable(name="build_query_node")
async def build_query_node(state: AgentState) -> AgentState:
    """Build MongoDB query based on schema and parsed request"""
    print(f"\n=== BUILD QUERY NODE ===")
    print(f"Database: {state.get('db', 'None')}")
    print(f"Selected collection: {state.get('collections', [])}")
    print(f"Available collections: {state.get('collections', [])}")
    
    if "error" in state and state["error"]:
        print(f"Retry mode - Previous error: {state['error']}")
        previous_query = state.get("query", "None")
        prompt = f"""
            Please fix the previously failed query.

            Previous error:
            {state['error']}

            Previous query:
            {previous_query}

            Schema:
            {json.dumps(state.get('schemas', {}), default=str)}

            User request:
            {state.get('parsed', '')}

            Return only a valid JSON query object that the executor can run directly.
            """
    else:
        print("Building initial query...")
        
        # Handle multi-collection schema   
        relations = (state.get('schemas', {}) or {}).get('relations', [])
        notes = (state.get('schemas', {}) or {}).get('notes', {})
        schema_data = state.get('schemas', {})
        if isinstance(schema_data, dict) and "collections" in schema_data:
            print(f"Multi-collection schema with {schema_data.get('total_collections', 0)} collections")
            for collection, info in schema_data["collections"].items():
                if isinstance(info, dict):
                    fields = (info.get("schema") or {})
                    field_names = list(fields.keys())[:5] if isinstance(fields, dict) else []
                    print(f"  {collection}: total_fields={info.get('total_fields', 0)}, sample_fields={field_names}")
        else:
            fields = (schema_data.get("schema") or {})
            field_names = list(fields.keys())[:10] if isinstance(fields, dict) else []
            print(f"Single collection schema: total_fields={schema_data.get('total_fields', 0)}, sample_fields={field_names}")
            # Calculate current date for "last 30 days" queries (not used in prompt, but printed for debugging)
            current_date = datetime.now()
            thirty_days_ago = current_date - timedelta(days=30)
            current_date_iso = current_date.isoformat() + "Z"
            thirty_days_ago_iso = thirty_days_ago.isoformat() + "Z"
            _ = (current_date_iso, thirty_days_ago_iso)

        # Brace-free prompt
        prompt = (
        "You are a Professional MongoDB query builder. Return ONE valid JSON object only; no prose or code fences.\n\n"
        f"Context:\n"
        f"- Database alias: {state.get('db','')}\n"
        f"- Selected collections: {state.get('collections', [])}\n"
        f"- Parsed user request (intent/constraints):\n{state.get('parsed','')}\n\n"
        "Schema catalog (use fields from here; do not invent):\n"
        f"{json.dumps(state.get('schemas', {}), default=str)}\n\n"
        "Detected relations (prefer these for $lookup joins):\n"
        f"{json.dumps(relations, default=str)}\n\n"
        "Guidance notes (primary candidates, joins, filters, sorts, aggregation hints, ambiguities, report_defaults):\n"
        f"{json.dumps(notes, default=str)}\n\n"
        "Task:\n"
        "- Pick the best primary collection for this specific user request.\n"
        "- Output shape (exactly one of):\n"
        "- Find:\n"
        '  {{ "primary_collection": "<collection>", "collection": "<collection>", "filters": {{}}, "projection"?: {{}}, "sort"?: {{}}, "limit": 100, "is_aggregate": false }}\n'
        "- Aggregate:\n"
        '  {{ "primary_collection": "<collection>", "collection": "<collection>", "pipeline": [{{"$match": {{...}}}}, ...], "is_aggregate": true }}\n'
        "- Build $lookup stages using relations: from, localField, foreignField, as. Use $unwind if needed.\n"
        "- Use only field names present in the target collection's schema.\n"
        "Rules for Query Building:\n"
        "- Mandatory Fields: include material_name, material_id, available_quantity, company_id, location_id.\n"
        '- Default Filters: add {{"isActive": true}} and {{"activeListing": true}} unless specified otherwise.\n'
        "- Location Logic: respect location_id if provided; otherwise follow default like default_location_id.\n"
        "IMPORTANT: Best Practices (Not strict rules, but important),Put $match and $project early → reduces documents/fields processed downstream, improving performance.Index usage → only $match at the beginning of the pipeline can use indexes effectively.Limit $unwind usage → if arrays are very large, $unwind can blow up the number of documents.Use $facet for multi-result pipelines → allows branching pipelines inside one aggregation.Use $project to rename/reshape documents → keeps only necessary fields.,create the pipeline in any order you think is best and efficient.\n"
        "Rule of thumb: You can repeat stages.Just remember: order matters — each stage transforms the docs passed from the previous one.\n"
        " use ISO-8601 dates; validate all fields.\n"
        "- If is_aggregate is true, include a non-empty pipeline array.\n\n"
        "Finally:\n"
        "- Output only the JSON object (find or aggregate). Do not include explanations or code fences.\n"
        )


        # 2) Default fallback query (so we always have something to return)
        parsed = {}
        collections = state.get("collections") or []
        fallback_collection = collections[0] if collections else ""
        primary_candidates = []
        if isinstance(notes, dict):
            primary_candidates = notes.get("primary_candidates", []) or []

        query = {
            "collection": fallback_collection,
            "filters": {},
            "limit": 100,
            "is_aggregate": False
        }

        try:
            # 3) Call LLM asynchronously
            with anyio.CancelScope() as scope:
                response = await llm.ainvoke(prompt)
                if scope.cancel_called:
                    raise Exception(str({"error": "cancelled"}))
            print(f"LLM response: {response.content}")

            # 4) Parse robustly
            content = (response.content or "").strip()
            if content.startswith("```json"): content = content[7:]
            if content.endswith("```"): content = content[:-3]
            content = content.strip()

            if content:
                # try to find a JSON object
                m = re.search(r'\{[\s\S]*\}', content)
                if m:
                    content = m.group(0)
                print(f"Cleaned content for JSON parsing: {content}")
                parsed = json.loads(content)

                # LLM-chosen primary (with safe fallbacks)
                primary = (
                    parsed.get("primary_collection") 
                    or parsed.get("targeted_collection") 
                    or parsed.get("collection")
                    or (primary_candidates[0] if primary_candidates else None)
                    or fallback_collection
                )
                if primary not in collections and fallback_collection:
                    primary = fallback_collection

                    
                # normalize shape
                if parsed.get("is_aggregate", False):
                    query = {
                        "primary_collection": primary,
                        "collection": primary,
                        "pipeline": parsed.get("pipeline", []),
                        "is_aggregate": True,
                    }
                else:
                    q = {
                        "primary_collection": primary,
                        "collection": primary,
                        "filters": parsed.get("filters", {}),
                        "limit": parsed.get("limit", 100),
                        "is_aggregate": False
                    }
                    # only include projection/sort if provided
                    if "projection" in parsed and parsed["projection"] is not None:
                        q["projection"] = parsed["projection"]
                    if "sort" in parsed and parsed["sort"] is not None:
                        q["sort"] = parsed["sort"]
                    query = q


            # log the effective query
            print(f"\n🔍 GENERATED MONGODB QUERY:")
            print(f"Collection: {query.get('collection','')}")
            if query.get("is_aggregate"):
                print(f"Pipeline: {query.get('pipeline', [])}")
            else:
                print(f"Filters: {query.get('filters', {})}")
                if "projection" in query:
                    print(f"Projection: {query['projection']}")
                if "sort" in query:
                    print(f"Sort: {query['sort']}")
                print(f"Limit: {query.get('limit', 100)}")
            print(f"Is Aggregate: {query.get('is_aggregate', False)}")

        except Exception as e:
            # keep fallback and expose reason
            print(f"Query build failed; using fallback. Error: {e}")

    # 5) Return result deterministically
    result = {
        "query": query,
        "is_aggregate": query.get("is_aggregate", False),
        "error": None
    }
    print("=== END BUILD QUERY NODE ===\n")

    if state.get("is_aggregate", False):
        pipeline = state["query"].get("pipeline", [])
        if not pipeline:
            return {"error": "Aggregate requested but no pipeline provided", "retry_count": state.get("retry_count", 0) + 1}

    return result