<<<<<<< HEAD
from langgraph_agent.state import AgentState
import json
import logging
from langsmith import traceable
import anyio
=======
from typing import TypedDict, Dict, Any, List, Optional
from langgraph_agent.state import AgentState
import asyncio
import json
import logging, os
from langsmith import traceable
>>>>>>> 223c614a58b7b3575c30fb085cd8d3454533adf0

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

@traceable(name="execute_query_node")
async def execute_query_node(state: AgentState) -> AgentState:
    """Execute the MongoDB query using LLM with MCP tools"""
    log_flow.info("NODE_START name=execute_query db=%s is_aggregate=%s", state.get("db"), state.get("is_aggregate"))
    print(f"\n=== EXECUTE QUERY NODE ===")
    print(f"Input state: {state}")
    
    try:
        alias = state["db"]
        collections = state.get("collections", [])
        primary_collection = state.get("query", {}).get("collection", "")
        
        if not primary_collection:
            raise ValueError(f"No collection specified in query. Available collections: {collections}")
        
        # System prompt for LLM with MCP tools
        system = f"""
            You can call tools to execute MongoDB queries for alias '{alias}'. 
            Use find_documents (alias, collection, filter, projection, sort, limit) or aggregate_documents (alias, collection, pipeline).
            Available collections: {collections}
            Primary collection: {primary_collection}
            Return only JSON in the schema: {{"result": {{query_result}}, "query_type": "find|aggregate", "summary": "..."}}
            When you call aggregate_documents , you must include the pipeline array in the tool call arguments, not only in your message content.
            """
        
        user = f"""
            User query: {state.get('input','')}
            Parsed request: {state.get('parsed','')}
            Available collections: {collections}
            Primary collection: {primary_collection}
            Query details: {state.get('query', {})}

            Execute the appropriate MongoDB query based on the query details provided.
            If it's an aggregate query with $lookup stages, use aggregate_documents with the pipeline.
            If it's a simple find query, use find_documents with the appropriate filters, projection, sort, and limit.
            """
        
        print(f"Executing query using MCP tools...")
        print(f"Collections: {collections}")
        print(f"Primary collection: {primary_collection}")
        print(f"Is aggregate: {state.get('is_aggregate', False)}")
        
        from mcp_server.tools.mcp_langchain_tools import llm_with_mcp_tools
<<<<<<< HEAD
        
        with anyio.CancelScope() as scope:
            ai = await llm_with_mcp_tools(system, user)
            if scope.cancel_called:
                raise Exception(str({"error": "cancelled"}))
=======
        ai = await llm_with_mcp_tools(system, user)
>>>>>>> 223c614a58b7b3575c30fb085cd8d3454533adf0
        content = (ai.content or "").strip()
        
        print(f"LLM response: {content}")
        
        try:
            # Clean the response to extract JSON
            if content.startswith("```json"): 
                content = content[7:]
            if content.endswith("```"): 
                content = content[:-3]
            content = content.strip()
            
            response_data = json.loads(content)
            result = response_data.get("result", {})
            query_type = response_data.get("query_type", "unknown")
            summary = response_data.get("summary", "No summary provided")
            
            print(f"Query executed successfully using {query_type}")
            print(f"Summary: {summary}")
            
            result_output = {
                "result": _json_safe(result),
                "query_type": query_type,
                "summary": summary,
                "error": None
            }
            
            print(f"Execute query output: {result_output}")
            print("=== END EXECUTE QUERY NODE ===\n")
            log_flow.info("NODE_END name=execute_query result_summary=%s", str(result_output.get("summary",""))[:200])
            return result_output
            
        except Exception as e:
            # Fallback if LLM response parsing fails
            result_output = {
                "error": f"LLM query execution failed: {str(e)}",
                "retry_count": state.get("retry_count", 0) + 1
            }
            print(f"Error: {result_output['error']}")
            print("=== END EXECUTE QUERY NODE ===\n")
            log_flow.info("NODE_ERROR name=execute_query error=%s", result_output["error"])
            return result_output
        
    except Exception as e:
        result_output = {
            "error": str(e),
            "retry_count": state.get("retry_count", 0) + 1
        }
        print(f"Execute query output: {result_output}")
        print("=== END EXECUTE QUERY NODE ===\n")
        log_flow.info("NODE_ERROR name=execute_query error=%s", result_output["error"])
        return result_output
