from langgraph_agent.state import AgentState
from mcp_server.tools.mcp_langchain_tools import llm_with_mcp_tools, _list_collections
import json
import logging
from langsmith import traceable
import anyio

log_flow = logging.getLogger("flow")
log_llm = logging.getLogger("llm")
log_mcp = logging.getLogger("mcp")
logger = logging.getLogger("app")

@traceable(name="choose_collection_node")
async def choose_collection_node(state: AgentState) -> AgentState:
    log_flow.info("NODE_START name=choose_collection db=%s", state.get("db"))
    """Get available collections and select the most relevant ones using LLM with MCP tools"""
    print(f"\n=== CHOOSE COLLECTION NODE ===")
    print(f"Database: {state.get('db', 'None')}")
    
    # Check if db is set
    if not state.get("db"):
        result = {
            "collections": [],
            "target_collection": "",
            "error": "No database selected"
        }
        print(f"Error: {result['error']}")
        print("=== END CHOOSE COLLECTION NODE ===\n")
        return result
    
    try:
        alias = state["db"]
        
        # System prompt for LLM with MCP tools
        system = f"""
        You can call tools to explore the MongoDB database for alias '{alias}'. 
        Use list_collections (alias, database?) to see available collections.
        Then optionally use collection_schema (alias, collection) to understand collection structure.
        Return only JSON in the schema: {{"selected_collections": [...], "reasoning": "..."}}
        
        IMPORTANT:“First call list_collections for alias ‘{alias}’. Copy the tool’s returned array into discovered_collections. Only select from discovered_collections. Return strictly JSON: <discovered_collections: [...], selected_collections: [...], reasoning: '...'>”
        Do not select collections that don't exist in this database.
        """
        
        user = f"""
        User query: {state.get('input','')}
        Parsed request: {state.get('parsed','')}
        Goal: Select 1-3 relevant collections for the user's query.
        
        
        RULES:
        First, call list_collections with alias='{alias}' to see what collections are available.
        Then, optionally call collection_schema for collections that look relevant to understand their structure.
        Make your decision based on the User query and Parsed request , the collections you select will be used to build the query.
        Finally, select the most relevant collections for the user's query and parsed request.
        
        IMPORTANT: Only select from the available collections list above!
        """
        
        print("Asking LLM to explore collections using MCP tools...")
        with anyio.CancelScope() as scope:
            ai = await llm_with_mcp_tools(system, user, max_steps=2)
            if scope.cancel_called:
                raise Exception(str({"error": "cancelled"}))
        content = (ai.content or "").strip()

        try:
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]
            sel = json.loads(content)

            discovered = sel.get("discovered_collections", []) or []
            selected = sel.get("selected_collections", []) or []

            # If the tool could not retrieve any collections, stop the graph here
            if not discovered:
                # Try a direct tool call to capture a concrete error from MCP
                try:
                    probe = await _list_collections(alias)
                    if not probe:
                        raise RuntimeError(
                            f"No collections retrievable for alias '{alias}'. "
                            "MCP returned an empty list."
                        )
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to retrieve collections from MCP for alias '{alias}': {e}"
                    )

            invalid = [c for c in selected if c not in discovered]
            if invalid:
                return {
                    "collections": [],
                    "error": f"Invalid collections selected: {invalid}. Available: {discovered}",
                    "redirect_to_parse": True,
                    "feedback": f"Choose only from: {discovered}",
                }

            # If the tool worked but the LLM picked none, also stop early
            if not selected:
                raise RuntimeError(
                    f"No collections selected despite MCP returning {len(discovered)} collections for alias '{alias}'."
                )

            print(f"Output: {len(selected)} collections selected")
            print("=== END CHOOSE COLLECTION NODE ===\n")
            return {"collections": selected,
            "redirect_to_parse": False,
            "feedback": ""
            }

        except Exception as e:
            result = {
                "collections": [],
                "error": f"LLM selection failed: {str(e)}",
            }
            print(f"Error: {result['error']}")
            print("=== END CHOOSE COLLECTION NODE ===\n")
            return result

    except Exception as e:
        result = {
            "collections": [],
            "error": f"Failed to get collections: {str(e)}"
        }
        print(f"Error: {result['error']}")
        print("=== END CHOOSE COLLECTION NODE ===\n")
        return result

