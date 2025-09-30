from langgraph_agent.state import AgentState
import json
import logging
from langsmith import traceable
import anyio
from mcp_server.tools.mcp_langchain_tools import llm_with_mcp_tools, _find_with_debug, _aggregate_with_debug

log_flow = logging.getLogger("flow")
log_llm = logging.getLogger("llm")
log_mcp = logging.getLogger("mcp")
logger = logging.getLogger("app")

def _json_safe(x):
    try:
        import json
        json.dumps(x)
        return x
    except Exception:
        # fallback stringify for odd types (ObjectId, datetime, etc.)
        return str(x)

MAX_EXEC_RETRIES = 2

@traceable(name="execute_query_node")
async def execute_query_node(state: AgentState) -> AgentState:
    print("\n=== EXECUTE QUERY NODE ===")
    alias = state.get("db")
    mgr = state.get("mcp_session_manager")
    if not mgr:
        print("ERROR: MCP session manager not available")
        return {"error": "MCP session manager not available", "retry_count": state.get("retry_count", 0) + 1}

    q = state.get("query") or {}
    collection = q.get("collection")
    if not collection:
        print("ERROR: No collection specified in query")
        return {"error": "No collection specified in query", "retry_count": state.get("retry_count", 0) + 1}

    is_aggr = bool(q.get("is_aggregate"))
    try:
        if is_aggr:
            pipeline = q.get("pipeline") or []
            if not isinstance(pipeline, list) or not pipeline:
                print("ERROR: Aggregate requested but pipeline missing/empty")
                return {
                    "error": "Aggregate requested but pipeline missing/empty",
                    "retry_count": state.get("retry_count", 0) + 1,
                    "feedback": "Pipeline missing or empty in aggregate request."
                }

            print(f"Executing aggregate on {collection} with {len(pipeline)} stages")
            result = await _aggregate_with_debug(mgr, alias, collection, pipeline, database=None)

            docs = result.get("documents", [])
            debug = result.get("debug_info", {})
            success = result.get("success", True)

            if not success or debug.get("final_count", 0) == 0:
                empty_after = debug.get("empty_after_stage")
                fb = (
                    f"No results after stage {empty_after}: {pipeline[empty_after]}"
                    if empty_after is not None and empty_after < len(pipeline)
                    else "No results returned; relax filters or check joins."
                )
                print(f"Retry #{state.get('retry_count', 0) + 1} → AGGREGATE EMPTY/ERROR. feedback={fb}")
                print(f"Debug: {json.dumps(debug, default=str)[:1200]}")
                return {
                    "error": "EMPTY_RESULT" if success else "AGGREGATE_FAILED",
                    "debug_info": debug,
                    "retry_count": state.get("retry_count", 0) + 1,
                    "feedback": fb,
                    "query": q,
                    "is_aggregate": True,
                    "failure_signature": {
                        "kind": "aggregate_empty",
                        "empty_after_stage": empty_after,
                        "first_stage_op": list((pipeline[empty_after] or {}).keys())[0] if (empty_after is not None and empty_after < len(pipeline)) else None,
                    }
                }

            print(f"AGGREGATE OK. count={debug.get('final_count', len(docs))}")
            print(f"Sample: {json.dumps((docs[:1] or [{}])[0], default=str)[:400]}")
            return {
                "result": {"documents": docs},
                "debug_info": debug,
                "query_type": "aggregate",
                "summary": f"Aggregate on {collection} returned {debug.get('final_count', len(docs))} documents",
                "error": None
            }

        else:
            filters = q.get("filters") or {}
            projection = q.get("projection")
            sort = q.get("sort")
            limit = q.get("limit", 100)
            print(f"Executing find on {collection} filters={filters} limit={limit}")
            result = await _find_with_debug(mgr, alias, collection, database=None,
                                            filter=filters, projection=projection, sort=sort, limit=limit)

            docs = result.get("documents", [])
            debug = result.get("debug_info", {})
            success = result.get("success", True)

            if not success or debug.get("document_count", 0) == 0:
                fb = f"No documents for filters={filters}. Consider relaxing filters or increasing limit."
                print(f"FIND EMPTY/ERROR. feedback={fb}")
                print(f"Debug: {json.dumps(debug, default=str)[:1200]}")
                return {
                    "error": "EMPTY_RESULT" if success else "FIND_FAILED",
                    "debug_info": debug,
                    "retry_count": state.get("retry_count", 0) + 1,
                    "feedback": fb,
                    "query": q,
                    "is_aggregate": False
                }

            print(f"FIND OK. count={debug.get('document_count', len(docs))}")
            print(f"Sample: {json.dumps((docs[:1] or [{}])[0], default=str)[:400]}")
            return {
                "result": {"documents": docs},
                "debug_info": debug,
                "query_type": "find",
                "summary": f"Find on {collection} returned {debug.get('document_count', len(docs))} documents",
                "error": None
            }

    except Exception as e:
        print(f"EXECUTE ERROR: {e}")
        return {"error": str(e), "retry_count": state.get("retry_count", 0) + 1}
