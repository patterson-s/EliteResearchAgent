import os
import json
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_prompt(prompt_path: Path) -> str:
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

def parse_json_response(text: str) -> Dict[str, Any]:
    text = text.strip()
    text = text.replace("```json", "").replace("```", "").strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"ERROR parsing JSON: {e}")
        print(f"Raw text: {text[:500]}")
        raise

def save_json(data: Dict[str, Any], filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_api_key(config: Dict[str, Any]) -> str:
    api_key = os.getenv(config["api_key_env_var"])
    if not api_key:
        raise EnvironmentError(f"Missing {config['api_key_env_var']} environment variable")
    return api_key