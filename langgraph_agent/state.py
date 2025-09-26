from typing import TypedDict, Dict, Any, List, Optional

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
<<<<<<< HEAD
    schemas: Dict[str, Any]  # Added for schema catalog
=======
>>>>>>> 223c614a58b7b3575c30fb085cd8d3454533adf0
    query: Dict[str, Any]
    is_aggregate: bool
    
    # Results and error handling
    result: Dict[str, Any]
    error: Optional[str]
<<<<<<< HEAD
    retry_count: int
    
    # Flow control
    redirect_to_choose: bool
    redirect_to_parse: bool
    feedback: str
=======
    retry_count: int
>>>>>>> 223c614a58b7b3575c30fb085cd8d3454533adf0
