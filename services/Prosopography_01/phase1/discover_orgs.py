"""Step 2: Discover and consolidate canonical organizations."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import Dict, Any, List, Optional
import json

from llm_client import LLMClient
from utils import load_prompt, load_config


def discover_canonical_orgs(
    organizations: List[Dict[str, Any]],
    llm_client: LLMClient,
    config: Optional[Dict[str, Any]] = None,
    roles: Optional[List[Dict[str, Any]]] = None,
    person_name: Optional[str] = None
) -> Dict[str, Any]:
    """Discover canonical organizations from extracted organization entities.

    Args:
        organizations: List of organization entities with name and quotes
        llm_client: LLM client instance
        config: Optional configuration
        roles: Optional list of role entities (helps infer employer organizations)
        person_name: Optional person name (helps infer nationality for diplomatic careers)

    Returns:
        Dictionary with canonical_organizations list
    """
    if config is None:
        config = load_config()

    # Even with no explicit organizations, roles can help us infer employers
    if not organizations and not roles:
        return {"canonical_organizations": []}

    system_prompt = load_prompt("phase1_canonical_orgs", config)

    # Format organizations for the prompt
    org_list = []
    for idx, org in enumerate(organizations or []):
        org_entry = {
            "index": idx,
            "name": org.get("name", ""),
            "quotes": org.get("quotes", [])[:2]  # Limit quotes to avoid token overflow
        }
        org_list.append(org_entry)

    # Format roles to help infer employer organizations
    role_list = []
    for idx, role in enumerate(roles or []):
        role_entry = {
            "index": idx,
            "title": role.get("title", ""),
            "quotes": role.get("quotes", [])[:2]
        }
        role_list.append(role_entry)

    # Build user prompt with both organizations and roles
    user_prompt = f"""Analyze these entities and identify canonical organizations.

PERSON: {person_name or "Unknown"}

ORGANIZATIONS EXPLICITLY MENTIONED:
{json.dumps(org_list, indent=2)}

ROLES/POSITIONS (use to INFER employer organizations):
{json.dumps(role_list, indent=2)}

CRITICAL: For diplomatic/government roles, you MUST infer the employing organization:
- "Diplomat", "Ambassador", "Embassy" roles → Ministry of Foreign Affairs - [Country]
- "Minister of X" → Ministry of X - [Country]
- Look at the person's nationality to determine which country's ministry

Return ONLY valid JSON with the canonical organizations mapping."""

    try:
        result = llm_client.generate_json(
            prompt=user_prompt,
            system_prompt=system_prompt
        )

        canonical_orgs = result.get("canonical_organizations", [])

        # Validate and clean up the result
        validated_orgs = []
        for org in canonical_orgs:
            validated_org = {
                "canonical_id": org.get("canonical_id", f"ORG_{len(validated_orgs)+1:03d}"),
                "canonical_name": org.get("canonical_name", ""),
                "org_type": org.get("org_type", "other"),
                "entity_indices": org.get("entity_indices", []),
                "variations_found": org.get("variations_found", []),
                "reasoning": org.get("reasoning", "")
            }
            if validated_org["canonical_name"]:
                validated_orgs.append(validated_org)

        return {"canonical_organizations": validated_orgs}

    except Exception as e:
        # Return each org as its own canonical on error
        fallback_orgs = []
        for idx, org in enumerate(organizations):
            fallback_orgs.append({
                "canonical_id": f"ORG_{idx+1:03d}",
                "canonical_name": org.get("name", ""),
                "org_type": "other",
                "entity_indices": [idx],
                "variations_found": [org.get("name", "")],
                "reasoning": f"Fallback due to error: {str(e)}"
            })
        return {"canonical_organizations": fallback_orgs}


def build_org_mapping(canonical_orgs: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Build a mapping from entity index to canonical organization.

    Args:
        canonical_orgs: List of canonical organizations

    Returns:
        Dictionary mapping entity index to canonical org info
    """
    mapping = {}
    for org in canonical_orgs:
        for idx in org.get("entity_indices", []):
            mapping[idx] = {
                "canonical_id": org["canonical_id"],
                "canonical_name": org["canonical_name"],
                "org_type": org.get("org_type", "other")
            }
    return mapping
