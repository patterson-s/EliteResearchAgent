import os
import re
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import cohere
import json
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path)

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def fill_template(template: str, variables: Dict[str, str]) -> str:
    text = template
    for key, value in variables.items():
        text = text.replace(f"{{{{{key}}}}}", str(value))
    return text

YEAR_PATTERN = re.compile(r"\b(1[6-9]\d{2}|20\d{2})\b")

def parse_extraction_output(text: str) -> Tuple[bool, Optional[int], str]:
    contains = False
    year = None
    reasoning = ""
    
    reasoning_match = re.search(
        r"reasoning:(.*?)contains_birthdate:",
        text,
        flags=re.DOTALL | re.IGNORECASE
    )
    if reasoning_match:
        reasoning = reasoning_match.group(1).strip()
    
    contains_match = re.search(
        r"contains_birthdate:\s*(true|false)",
        text,
        flags=re.IGNORECASE
    )
    if contains_match and contains_match.group(1).lower() == "true":
        contains = True
    
    year_match = re.search(
        r"birth_year:\s*(null|\d{4})",
        text,
        flags=re.IGNORECASE
    )
    if year_match:
        val = year_match.group(1).lower()
        if val != "null":
            try:
                y = int(val)
                if 1600 <= y <= 2099:
                    year = y
            except Exception:
                pass
    
    if contains and year is None:
        year_in_text = YEAR_PATTERN.search(text)
        if year_in_text:
            y = int(year_in_text.group(0))
            if 1600 <= y <= 2099:
                year = y
    
    return contains, year, reasoning

def classify_evidence_type(text: str) -> str:
    t = text.lower()
    if "date of birth" in t or "place and date of birth" in t:
        return "born-field"
    if "born" in t or "née" in t or "né " in t or " b. " in t:
        return "born-narrative"
    if " births" in t or ("births" in t and "category" in t):
        return "category"
    return "other"

def extract_birth_year(
    person_name: str,
    chunk_text: str,
    chunk_id: int,
    config_path: Path
) -> Dict[str, Any]:
    config = load_config(config_path)
    api_key = os.getenv(config["api_keys"]["cohere"])
    
    if not api_key:
        raise EnvironmentError(f"Missing {config['api_keys']['cohere']} environment variable")
    
    co = cohere.Client(api_key)
    
    script_dir = Path(__file__).parent
    system_prompt = load_prompt(script_dir / config["prompts"]["system_prompt_path"])
    user_template = load_prompt(script_dir / config["prompts"]["user_prompt_path"])
    
    user_prompt = fill_template(user_template, {
        "PERSON_NAME": person_name,
        "CHUNK_TEXT": chunk_text
    })
    
    response = co.chat(
        model=config["extraction"]["model"],
        temperature=config["extraction"]["temperature"],
        preamble=system_prompt,
        message=user_prompt,
        max_tokens=config["extraction"]["max_tokens"]
    )
    
    raw_output = response.text.strip()
    contains, year, reasoning = parse_extraction_output(raw_output)
    evidence_type = classify_evidence_type(chunk_text) if contains else None
    
    return {
        "chunk_id": chunk_id,
        "person_name": person_name,
        "contains_birth_info": contains,
        "extracted_year": year,
        "evidence_type": evidence_type,
        "reasoning": reasoning,
        "raw_llm_output": raw_output
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract birth year from a single chunk")
    parser.add_argument("--person", required=True)
    parser.add_argument("--chunk-id", type=int, required=True)
    parser.add_argument("--text", required=True)
    parser.add_argument("--config", type=Path, default=Path("config/config.json"))
    args = parser.parse_args()
    
    result = extract_birth_year(args.person, args.text, args.chunk_id, args.config)
    
    print("\n" + "=" * 80)
    print(f"Extraction Result for chunk {args.chunk_id}")
    print("=" * 80)
    print(f"Contains birth info: {result['contains_birth_info']}")
    print(f"Extracted year: {result['extracted_year']}")
    print(f"Evidence type: {result['evidence_type']}")
    print(f"\nReasoning:\n{result['reasoning']}")