from pydantic import BaseModel
from typing import Optional, Dict, Any, List

from pydantic import BaseModel
from typing import Optional, Dict, Any, List


# Pydantic models for request/response
class QueryRequest(BaseModel):
    db: Optional[str] = "Inventory"
    collection: str
    filters: Optional[Dict[str, Any]] = None
    projection: Optional[Dict[str, Any]] = None
    sort: Optional[Dict[str, int]] = None
    limit: int = 1000

class AggregateRequest(BaseModel):
    db: Optional[str] = "Inventory"
    collection: str
    pipeline: List[Dict[str, Any]]

class FindRequest(BaseModel):
    db: Optional[str] = "Inventory"
    collection: str
    filters: Optional[Dict[str, Any]] = None
    projection: Optional[Dict[str, Any]] = None
    sort: Optional[Dict[str, int]] = None
    skip: int = 0
    limit: int = 1000

class GraphQueryRequest(BaseModel):
    query: str