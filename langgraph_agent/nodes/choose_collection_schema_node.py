
from typing import Dict, Any, List
from langgraph_agent.state import AgentState
from config.settings import settings
from mcp_server.session_manager import RequestScopedSessionManager
from mcp_server.tools.mcp_langchain_tools import llm_with_mcp_tools
import json
import re
from langsmith import traceable
import anyio

async def _json_from_ai_content(text: str) -> Dict[str, Any]:
    if not text:
        return {}
    t = text.strip()
    if t.startswith("```json"): t = t[7:]
    if t.endswith("```"): t = t[:-3]
    t = t.strip()
    try:
        return json.loads(t)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", t)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return {}

@traceable(name="choose_collection_schema_node")
async def choose_collection_schema_node(state: AgentState) -> AgentState:
    print("\n=== CHOOSE COLLECTION SCHEMA NODE ===")
    
    # Get session manager from state
    mgr = state.get("mcp_session_manager")
    if not mgr:
        return {
            "error": "MCP session manager not found",
            "redirect_to_parse": True,
            "feedback": "MCP session manager not found",
        }
    
    alias = state.get("db") or "Inventory"
    parsed_text = state.get("parsed") or ""
    hinted = []
    try:
        hinted = (json.loads(parsed_text or "{}").get("relevant_collections") or [])
    except Exception:
        pass
    hinted = list({c for c in hinted if isinstance(c, str)})

    base_system = (
        "You are part of a MongoDB Query Generator. "
        "Your role is to analyze user intent, discover relevant collections, "
        "inspect schemas, infer relationships, and enrich field details to guide query construction. "
        "\n\n"
        "You can call tools in real time to confirm schema knowledge: "
        "- Use list_collections(alias, database?) to see what collections are available. "
        "- Use collection_schema(alias, collection) to inspect fields and types. "
        "\n\n"
        "When producing output: "
        "- Always call list_collections(alias) first and copy the returned array into discovered_collections. "
        "- Only select from discovered_collections when choosing relevant collections. "
        "- Return JSON ONLY in the required schema (strict, no extra text). "
        "- Do not invent collections or fields. "
        "- Only include fields validated by tools or visible in provided schemas. "
        "- Ensure relations and notes are concrete, not vague guesses."
    )

   # Fix the base_user_template by properly separating f-strings from the JSON template
    base_user_template = (
            "User query: {input}\n"
            "Parsed intent: {parsed}\n\n"
            "Steps for you:\n"
            "1. Call list_collections(alias='{alias}') to discover all available collections.\n"
            "2. Copy the result into \"discovered_collections\".\n"
            "3. Select 1–3 collections most relevant to the query and add them to \"selected_collections\".\n"
            "4. For those selected collections, inspect schemas with collection_schema and (if needed) sample with find_documents.\n"
            "5. Enrich schema details: field types, examples, nullable, key fields, date fields, enums, booleans, product/company/location fields.\n"
            "6. Infer relationships between collections (via *_id, <name>_id, ObjectId references).\n"
            "7. Produce structured notes for query building: primary_candidates, recommended_joins, natural_filters, useful_sorts, aggregation_hints, ambiguities, missing_info, field_aliases, id_types, report_defaults.\n\n"
            "Your final output must be STRICT JSON in this format:\n\n"
            "{{\n"
            "  \"discovered_collections\": [\"<list returned from list_collections>\"],\n"
            "  \"selected_collections\": [\"<subset relevant to query>\"],\n"
            "  \"schemas\": {{\n"
            "    \"<collection>\": {{\n"
            "      \"schema\": {{}},\n"
            "      \"total_fields\": 0,\n"
            "      \"field_names\": []\n"
            "    }}\n"
            "  }},\n"
            "  \"relations\": [\n"
            "    {{\"from\": \"<collection>\", \"localField\": \"<local_field>\", \"foreign\": \"<collection>\", \"foreignField\": \"<remote_field>\", \"as\": \"<joinedField>\", \"cardinality\": \"one-to-one|one-to-many|many-to-one\"}}\n"
            "  ],\n"
            "  \"notes\": {{\n"
            "    \"primary_candidates\": [\"<collection>\", ...],\n"
            "    \"joins\": [\"A.localID -> B._id\", ...],\n"
            "    \"filters\": [\"date range on <field>\", \"company_id equals ...\"],\n"
            "    \"sorts\": [\"-createdAt\", \"quantity desc\"],\n"
            "    \"aggregation_hints\": [\"group by <field>\", \"lookup <collection> on <field>\"],\n"
            "    \"ambiguities\": [\"<field> appears in multiple collections; disambiguate by ...\"],\n"
            "    \"report_defaults\": {{\n"
            "      \"projection\": [\"material_name\", \"material_id\", \"available_quantity\"],\n"
            "      \"filters\": {{\"isActive\": true, \"activeListing\": true}},\n"
            "      \"location_behavior\": \"If no location specified, use `default_location_id`.\"\n"
            "    }}\n"
            "  }},\n"
            "  \"relevance_ok\": true,\n"
            "  \"preferred_collections\": [\"<col>\", \"...\"],\n"
            "  \"reason\": \"why these collections are suitable\"\n"
            "}}"
        )
    def build_user(extra_feedback: str = "") -> str:
        core = base_user_template.format(
            input=state.get("input", ""),
            parsed=parsed_text or "(no parsed intent available)",
            alias=alias,
        )
        if hinted:
            core += f"\nHints from previous parsing (if any): {json.dumps(hinted)}\n"
        if extra_feedback:
            core += f"\nIMPORTANT FEEDBACK: {extra_feedback}\n"
        return core

    def normalize_and_check(data: Dict[str, Any]) -> Dict[str, Any]:
        discovered = data.get("discovered_collections") or []
        if not isinstance(discovered, list): discovered = []
        discovered = [c for c in discovered if isinstance(c, str)]

        selected = data.get("selected_collections") or []
        if not isinstance(selected, list): selected = []
        selected = [c for c in selected if isinstance(c, str)]

        # Accept either "schemas" or older "schema" key
        schemas_in = data.get("schemas")
        if not isinstance(schemas_in, dict):
            schemas_in = data.get("schema") or {}
            if not isinstance(schemas_in, dict):
                schemas_in = {}

        # Enforce selected ⊆ discovered
        if discovered:
            selected = [c for c in selected if c in discovered]

        normalized = {}
        for col, sch in (schemas_in or {}).items():
            sdict = sch or {}
            sc = sdict.get("schema") if isinstance(sdict, dict) else {}
            if not isinstance(sc, dict):
                sc = {}
            normalized[col] = {
                "collection": col,
                "schema": sc,
                "total_fields": len(list(sc.keys())),
                "field_names": list(sc.keys())[:10],
            }

        relevance_ok = bool(data.get("relevance_ok", True))
        preferred = data.get("preferred_collections") or []
        reason = data.get("reason") or ""
        relations = data.get("relations") or []
        notes = data.get("notes") or {
            "primary_candidates": (selected[:1] if selected else []),
            "joins": [],
            "filters": [],
            "sorts": [],
            "aggregation_hints": [],
            "ambiguities": [],
            "report_defaults": {},
        }

        return {
            "discovered": discovered,
            "selected": selected,
            "schemas_norm": normalized,
            "relations": relations,
            "notes": notes,
            "relevance_ok": relevance_ok,
            "preferred": preferred,
            "reason": reason,
        }

    # Retry up to 3 plans if the model marks irrelevance or we detect invalid/empty selections/schemas
    max_attempts = 3
    feedback = ""
    for attempt in range(1, max_attempts + 1):
        try:
            system = base_system
            user = build_user(extra_feedback=feedback)

           
            
            with anyio.CancelScope() as scope:
                ai = await llm_with_mcp_tools(mgr, system, user)
                if scope.cancel_called:
                    raise Exception(str({"error": "cancelled"}))

            data = await _json_from_ai_content(ai.content or "")
            check = normalize_and_check(data)

            selected = check["selected"]
            schemas_norm = check["schemas_norm"]
            relevance_ok = check["relevance_ok"]
            preferred = check["preferred"]
            reason = check["reason"]

            # Validate outcome
            invalid = False
            problems = []

            if not selected:
                invalid = True
                problems.append("No valid collections selected (must be subset of discovered_collections).")

            if not schemas_norm or all(not (v.get("schema") or {}) for v in schemas_norm.values()):
                invalid = True
                problems.append("Schemas are empty or missing for selected collections.")

            if not relevance_ok:
                invalid = True
                problems.append("Model marked relevance_ok=false.")

            if not invalid:
                schema_catalog = {
                    "collections": schemas_norm,
                    "total_collections": len(schemas_norm),
                    "database": alias,
                    "relations": check["relations"],
                    "notes": check["notes"],
                }
                print("=== END CHOOSE COLLECTION SCHEMA NODE ===\n")
                return {
                    "collections": selected,
                    "schemas": schema_catalog,
                    "redirect_to_parse": False,
                    "redirect_to_choose": False,
                    "feedback": "",
                    "error": None,
                }

            # Prepare feedback and retry if attempts remain
            if attempt < max_attempts:
                feedback = (
                    "Your previous plan was not acceptable. Reasons: "
                    + "; ".join(problems)
                    + (f". Preferred alternatives: {preferred}." if preferred else "")
                    + (f" Additional note: {reason}" if reason else "")
                    + " Retry with different, better-suited collections strictly drawn from discovered_collections. "
                      "Ensure schemas are non-empty and reflect real fields. Set relevance_ok=true only if confident."
                )
                continue

            # Final failure after retries → redirect
            fb = "; ".join(problems)
            if preferred:
                fb += f" Preferred: {preferred}."
            if reason:
                fb += f" {reason}"
            return {
                "collections": selected,
                "schemas": {},
                "error": "Single-call discovery failed after retries",
                "redirect_to_choose": True,
                "feedback": fb.strip(),
            }

        except Exception as e:
            if attempt < max_attempts:
                feedback = f"Error occurred: {e}. Retry with a robust plan."
                continue
            return {
                "collections": [],
                "schemas": {},
                "error": f"Failed in single-call discovery: {e}",
                "redirect_to_parse": True,
                "feedback": "Discovery failed; please rephrase or try different intent.",
            }