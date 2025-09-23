from langchain_openai import ChatOpenAI
from langgraph_agent.state import AgentState
import json
import re
from config.settings import settings
from datetime import datetime, timedelta
from langsmith import traceable

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
{json.dumps(state.get('schema', {}), default=str)}

User request:
{state.get('parsed', '')}

Return only a valid JSON query object that the executor can run directly.
"""
    else:
        print("Building initial query...")
        
        # Handle multi-collection schema
        schema_data = state.get('schema', {})
        if isinstance(schema_data, dict) and "collections" in schema_data:
            # Multi-collection schema
            print(f"Multi-collection schema with {schema_data.get('total_collections', 0)} collections")
            
            # Show schema info for each collection
            for collection, schema in schema_data["collections"].items():
                if "error" not in schema:
                    schema_info = {
                        "total_fields": schema.get("total_fields", 0),
                        "field_names": list(schema.get("schema", {}).keys())[:5]
                    }
                    print(f"  {collection}: {schema_info}")
        else:
            # Single collection schema (fallback)
            schema_info = {
                "total_fields": schema_data.get("total_fields", 0),
                "field_names": list(schema_data.get("schema", {}).keys())[:10]
            }
            print(f"Single collection schema: {schema_info}")
        
        # Calculate current date for "last 30 days" queries (not used in prompt, but printed for debugging)
        current_date = datetime.now()
        thirty_days_ago = current_date - timedelta(days=30)
        current_date_iso = current_date.isoformat() + "Z"
        thirty_days_ago_iso = thirty_days_ago.isoformat() + "Z"
        _ = (current_date_iso, thirty_days_ago_iso)

        # Brace-free prompt
        prompt = f"""
You are a MongoDB query builder. Return one valid JSON object only. Do not include prose or code fences.

Context:
- Database alias: {state.get('db','')}
- Selected collections: {state.get('collections', [])}
- Parsed user request (intent and constraints):
{state.get('parsed','')}

Schema catalog (validate all field names against this; do not invent fields):
{json.dumps(state.get('schema', {}), default=str)}

Task:
- Analyze the user query and available collections to select the BEST primary collection for this specific query
- Always include an extra top-level field called primary collection that you selected ,in the output
  - "primary_collection": the chosen primary collection(string)
- Build a single MongoDB query JSON object the executor can run directly
- Use your selected primary collection as the main target
- For multi-collection queries, use aggregation with $lookup stages to join related collections
- Use aggregation when: grouping, lookups, complex date math, or when data from multiple collections is needed
- Use only field names that appear in the schema for the target collection
- Map natural language constraints to MongoDB operators. For example:
  greater than -> dollar-gt
  less than -> dollar-lt
  at least -> dollar-gte
  at most -> dollar-lte
  equals -> direct match
- Dates must be ISO-8601 strings. Example date range:
  start ISO string at 00:00:00 Z
  end ISO string at 23:59:59 Z
- For multi-collection queries, use $lookup to join related collections:
- Use $lookup with from: "collection_name", localField: "field_name", foreignField: "_id", as: "joined_data"
- After $lookup, use $unwind if you need to flatten the joined data
- Then apply $group, $project, $sort as needed
- Do not include keys with null values. If projection or sort are not needed, omit them completely.
- Also execute an aggregate query if the schema shows multi-collection context , so that we get the actual documents instead of just the ids 
- Keep limit reasonable (default 100 if unspecified).

Output JSON options (describe verbally; you must still output a proper JSON object):
- Find shape:
  keys: collection (string), filters (object), optional projection (object), optional sort (object), limit (integer), is_aggregate (false)
- Aggregate shape:
  keys: collection (string), pipeline (array of pipeline stages), is_aggregate (true)


Rules and validation:
- Choose the primary collection that best matches the user's query intent
- Consider which collection has the most relevant fields for the query
- For aggregation queries, choose the collection that contains the main data you want to group/analyze
- Validate every field name against the target collection schema
- If the schema shows a numeric type for an ID (for example company_id), output it as a number, not a quoted string.
- For booleans (for example isActive), use true or false literals.
- For ranges: if request says greater than A and less than B, use gt A and lt B. If phrased as at least or at most, use gte or lte respectively.
- If the request includes a timeframe, translate to an ISO date range on the most appropriate date field in the target schema (for example inventory_date for inventory, created_date for creation).
- If you set is_aggregate to true, include a non-empty pipeline array with valid stages ($match, $group, $project, $sort, etc.).
- When you call aggregate_documents , you must include the pipeline array in the tool call arguments, not only in your message content.

Finally:
- Output only the JSON object implementing either the find or aggregate shape. Do not include any explanation or code fences.
"""


    # 2) Default fallback query (so we always have something to return)
    parsed = {}
    collections = state.get("collections") or []
    fallback_collection = collections[0] if collections else ""

    query = {
        "collection": fallback_collection,
        "filters": {},
        "limit": 100,
        "is_aggregate": False
    }

    try:
        # 3) Call LLM asynchronously
        response = await llm.ainvoke(prompt)
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
                or fallback_collection
            )
            if primary not in collections and fallback_collection:
                primary = fallback_collection

                
            # normalize shape
            if parsed.get("is_aggregate", False):
                query = {
                    "collection": parsed.get("primary_collection", ""),
                    "pipeline": parsed.get("pipeline", []),
                    "is_aggregate": True,
                }
            else:
                q = {
                    "collection": parsed.get("primary_collection", ""),
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