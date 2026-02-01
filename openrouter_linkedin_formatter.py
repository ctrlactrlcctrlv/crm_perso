import json
from typing import List, Dict, Optional

import requests


def _build_prompt(profiles: List[str]) -> str:
    joined = "\n".join(f"- {p}" for p in profiles)
    return (
        "Tu vas recevoir une liste de profils LinkedIn (url format texte brut). "
        "Ta mission est de convertir chaque profil en un objet JSON strict "
        "avec les champs: prenom, nom, fonction, linkedin, entreprise.\n"
        "Contraintes:\n"
        "- Retourne UNIQUEMENT du JSON valide (pas de markdown, pas de texte).\n"
        "- Si un champ est inconnu, retourne une chaine vide \"\".\n"
        "- Le champ linkedin doit contenir une URL si disponible.\n\n"
        "Liste des profils:\n"
        f"{joined}"
    )


def formatter_profils_linkedin(
    profiles: List[str],
    api_key: str,
    model: str,
    timeout: int = 60,
) -> List[Dict[str, Optional[str]]]:
    if not profiles:
        return []

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    prompt = _build_prompt(profiles)
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        'plugins': [{'id': 'web', 'max_results': 3}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "linkedin_profiles",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "profiles": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "prenom": {"type": "string"},
                                    "nom": {"type": "string"},
                                    "fonction": {"type": "string"},
                                    "linkedin": {"type": "string"},
                                    "entreprise": {"type": "string"},
                                },
                                "required": [
                                    "prenom",
                                    "nom",
                                    "fonction",
                                    "linkedin",
                                    "entreprise",
                                ],
                                "additionalProperties": False,
                            },
                        }
                    },
                    "required": ["profiles"],
                    "additionalProperties": False,
                },
            },
        },
    }

    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json=data,
        timeout=timeout,
    )
    if response.status_code >= 300:
        raise RuntimeError(
            f"OpenRouter error: {response.status_code} {response.text}"
        )

    content = response.json()["choices"][0]["message"]["content"]
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON returned by OpenRouter: {content}") from exc

    if not isinstance(parsed, dict) or "profiles" not in parsed:
        raise ValueError("OpenRouter JSON must be an object with 'profiles'.")
    profiles = parsed["profiles"]
    if not isinstance(profiles, list):
        raise ValueError("'profiles' must be a list.")
    return profiles

# Example usage:
if __name__ == "__main__":
    test_profiles = [
        "https://www.linkedin.com/in/swann-fabre-b57587253/",
        "https://www.linkedin.com/in/mohamed-aouiti-trabelsi/",
    ]
    api_key = "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"
    model = "perplexity/sonar-pro-search"

    formatted_profiles = formatter_profils_linkedin(
        test_profiles, api_key, model
    )
    print(json.dumps(formatted_profiles, indent=2, ensure_ascii=False))
