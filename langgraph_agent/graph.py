from langgraph.graph import StateGraph, END
from langgraph_agent.state import AgentState
from langgraph_agent.nodes.parse_request_node import parse_request_node
from langgraph_agent.nodes.choose_collection_node import choose_collection_node
from langgraph_agent.nodes.get_schema_node import get_schema_node
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

import logging
log_flow = logging.getLogger("flow")

def should_redirect_to_parse(state: AgentState) -> str:
    """Check if we should redirect back to parse node"""
    if state.get("redirect_to_parse"):
        log_flow.info("ROUTING choose_collection -> parse_request (feedback present)")
        return "parse_request"
    else:
        return "get_schema"

@traceable(name="build_graph")
def build_graph():
    """Build the new linear graph with retry logic and redirect capability"""
    graph = StateGraph(AgentState)
    
    # Add nodes
    graph.add_node("parse_request", parse_request_node)
    graph.add_node("choose_collection", choose_collection_node)
    graph.add_node("get_schema", get_schema_node)
    graph.add_node("build_query", build_query_node)
    graph.add_node("execute_query", execute_query_node)
    
    # Linear flow with redirect capability
    graph.set_entry_point("parse_request")
    graph.add_edge("parse_request", "choose_collection")
    
    # Add conditional edge after choose_collection to handle redirects
    graph.add_conditional_edges(
        "choose_collection",
        should_redirect_to_parse,
        {
            "parse_request": "parse_request",
            "get_schema": "get_schema"
        }
    )
    
    graph.add_edge("get_schema", "build_query")
    graph.add_edge("build_query", "execute_query")
    
    # Replace the three problematic edges with one conditional edge
    graph.add_conditional_edges(
        "execute_query",
        should_retry,
        {
            "end": END,
            "retry": "build_query"
        }
    )
    
    return graph.compile()