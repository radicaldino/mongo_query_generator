from langchain_openai import ChatOpenAI
from langgraph_agent.state import AgentState
from config.database import get_db_aliases
from config.settings import settings
import json
import logging
import re
from utils import *
from langsmith import traceable

log_flow = logging.getLogger("flow")

OPENAI_API_KEY = settings.OPENAI_API_KEY
OPENAI_MODEL = settings.OPENAI_MODEL

llm = ChatOpenAI(api_key=OPENAI_API_KEY, model=OPENAI_MODEL, temperature=0.1)

@traceable(name="parse_request_node")
async def parse_request_node(state: AgentState) -> AgentState:
    """Parse user request to extract intent and target database"""
    print(f"\n=== PARSE REQUEST NODE ===")
    log_flow.info("NODE_START parse_request_node state_keys=%s", state)
    print(f"Input state: {state}")
    
    available_dbs = get_db_aliases()
    print(f"Available databases: {available_dbs}")
    
    # Get relations for ALL databases to provide complete context
    all_relations = {}
    if available_dbs:
        try:
            from utils.relations import get_schema_relation

            for db in available_dbs:
                # Remove await - this is a synchronous function
                relations_result = get_schema_relation(db=db)
                if relations_result.get("success"):
                    all_relations[db] = relations_result["relations"]
                    print(f"Loaded relations for {db}: {relations_result['total_schemas']} schemas")
                else:
                    print(f"Failed to load relations for {db}: {relations_result.get('error', 'Unknown error')}")
        except Exception as e:
            print(f"Failed to load relations: {str(e)}")
    
    # Check if this is a redirect with feedback
    feedback = state.get("feedback", "")
    if feedback:
        print(f"Received feedback: {feedback}")
    
    # Let the LLM do the database selection with complete relations context
    prompt = f"""
    User request: {state['input']}
    Available databases: {available_dbs}
    
    COMPLETE DATABASE RELATIONS CONTEXT:
    {json.dumps(all_relations, default=str, indent=2)}
    
    
    {f"FEEDBACK FROM PREVIOUS ATTEMPT: {feedback}" if feedback else ""}
    
    Analyze the request and determine which database is most relevant.
    Use the relations context to understand:
    - Which collections exist in each database
    - How collections are connected via foreign keys
    - What each collection contains based on descriptions
    - Which database best matches the user's intent
    - Choose only the collections that are available in the {json.dumps(all_relations, default=str, indent=2)}
    
    {f"IMPORTANT: {feedback}" if feedback else ""}
    
    Return ONLY a JSON response in this exact format:
    {{
        "selected_database": "DatabaseName",
        "reasoning": "Why this database was seleprint(json.dumps(all_relations, default=str, indent=2))cted based on the request and available collections/relations",
        "relevant_collections": ["collection1", "collection2", ...],
        "query_intent": "What the user wants to find/analyze",
        "suggested_relationships": ["collection1.collection2 via foreign_key", ...]
    }}
    
    Choose from these exact database names: {available_dbs}
    """
    response = llm.invoke(prompt)
    
    try:
        # Extract JSON from response (handle code fences)
        content = response.content.strip()
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
        
        result = json.loads(content)
        selected_db = result.get("selected_database", "")
        
        # Validate the selection
        if selected_db not in available_dbs:
            print(f"LLM selected invalid database: {selected_db}, using fallback")
            selected_db = available_dbs[0]
        
        print(f"Selected database: {selected_db}")
        print(f"Reasoning: {result.get('reasoning', 'No reasoning provided')}")
        print(f"Relevant collections: {result.get('relevant_collections', [])}")
        print(f"Query intent: {result.get('query_intent', 'Not specified')}")
        print(f"Suggested relationships: {result.get('suggested_relationships', [])}")
        
    except Exception as e:
        print(f"Failed to parse LLM response: {str(e)}")
        # Fallback: simple word matching
        user_query_lower = state['input'].lower()
        selected_db = available_dbs[0]
        
        for db in available_dbs:
            if db.lower() in user_query_lower:
                selected_db = db
                print(f"Fallback match found: {db}")
                break
    
    result = {
        "parsed": response.content,
        "db": selected_db,
        "relations_context": all_relations
    }
    
    # Clear any redirect flags
    if "redirect_to_parse" in result:
        del result["redirect_to_parse"]
    if "feedback" in result:
        del result["feedback"]
#only need to print the parsed and db from the result , no need for relations_context
    print(f"Parse request output: parsed: {result['parsed']}, db: {result['db']}")
    print("=== END PARSE REQUEST NODE ===\n")
    log_flow.info("NODE_END parse_request_node result=%s", list(result.keys()))
    
    return result
