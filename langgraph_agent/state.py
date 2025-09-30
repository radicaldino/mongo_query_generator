from typing import TypedDict, Dict, Any, List, Optional
from mcp_server.session_manager import RequestScopedSessionManager

class AgentState(TypedDict, total=False):
    # Input
    input: str  # user query
    parsed: str  # LLM parsed request
    
    # Database selection
    db: str
    target_collection: str
    collections: List[str]
    
    # Schema and query
    schema: Dict[str, Any]
    schemas: Dict[str, Any]  # Added for schema catalog

    query: Dict[str, Any]
    is_aggregate: bool
    
    # Results and error handling
    result: Dict[str, Any]
    error: Optional[str]
    retry_count: int
    debug_info: Dict[str, Any] 
    
    # Flow control
    redirect_to_choose: bool
    redirect_to_parse: bool
    feedback: str
    
    # Session management
    mcp_session_manager: Optional[RequestScopedSessionManager]
    

