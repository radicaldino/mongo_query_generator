from langgraph.graph import StateGraph, END
from langgraph_agent.state import AgentState
from langgraph_agent.nodes.parse_request_node import parse_request_node
from langgraph_agent.nodes.choose_collection_schema_node import choose_collection_schema_node
from langgraph_agent.nodes.build_query_node import build_query_node
from langgraph_agent.nodes.execute_query_node import execute_query_node
from langsmith import traceable
import logging, os

os.makedirs("./logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler("./logs/agent.log"),
        logging.StreamHandler()
    ],
)
log_flow = logging.getLogger("flow")
log_llm = logging.getLogger("llm")
log_mcp = logging.getLogger("mcp")
logger = logging.getLogger("app")

def should_retry(state: AgentState) -> str:
    """Determine if we should retry or end"""
    if not state.get("error"):
        return "end"
    elif state.get("retry_count", 0) >= 5:
        return "end"
    else:
        return "retry"

def _route_after_choose_collection_schema(state: AgentState) -> str:
    if state.get("redirect_to_parse"):
        log_flow.info("ROUTING choose_and_enrich -> parse_request (feedback present)")
        return "parse_request"
    if state.get("redirect_to_choose"):
        log_flow.info("ROUTING choose_and_enrich -> choose_and_enrich (retry selection/schema)")
        return "choose_and_enrich"
    return "build_query"

def build_graph():
    """Build the graph with merged choose+schema node"""
    graph = StateGraph(AgentState)
    
    # Nodes
    graph.add_node("parse_request", parse_request_node)
    graph.add_node("choose_collection_schema", choose_collection_schema_node)
    graph.add_node("build_query", build_query_node)
    graph.add_node("execute_query", execute_query_node)
    
    # Flow
    graph.set_entry_point("parse_request")
    graph.add_edge("parse_request", "choose_collection_schema")

    graph.add_conditional_edges(
        "choose_collection_schema",
        _route_after_choose_collection_schema,
        {
            "parse_request": "parse_request",
            "choose_collection_schema": "choose_collection_schema",
            "build_query": "build_query",
        },
    )

    graph.add_edge("build_query", "execute_query")

    graph.add_conditional_edges(
        "execute_query",
        should_retry,
        {
            "end": END,
            "retry": "build_query"
        }
    )
    
    return graph.compile()