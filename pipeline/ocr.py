import base64
import json
import mimetypes
import re
import time
from pathlib import Path
from typing import Dict, Optional

from openai import OpenAI

MAX_RETRIES = 3
BASE_SLEEP = 1.0


def image_to_data_url(path: Path) -> str:
    mime = mimetypes.guess_type(path)[0] or "image/jpeg"
    b64 = base64.b64encode(path.read_bytes()).decode("utf-8")
    return f"data:{mime};base64,{b64}"


def call_openai_with_retry(create_fn, *args, **kwargs):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return create_fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            time.sleep(BASE_SLEEP * attempt)
    raise RuntimeError(f"OpenAI request failed after retries: {last_err}")


def extract_json_str(s: str) -> Optional[str]:
    m = re.search(r"\{.*\}", s or "", re.S)
    return m.group(0) if m else None


def ocr_and_classify(
    client: OpenAI,
    model: str,
    categories_en,
    image_path: Path,
) -> Dict[str, str]:
    data_url = image_to_data_url(image_path)


    system_prompt = (
        "You are a strict OCR extractor and classifier for Bulgarian bank ads.\n"
        "Extract EXACT visible text (no guessing, no corrections).\n"
        "Classify into exactly ONE category from the list.\n"
        "Return ONLY valid JSON: {\"text\": string, \"type\": string}."
    )

    user_prompt = "Categories:\n- " + "\n- ".join(categories_en)

    resp = call_openai_with_retry(
        client.responses.create,
        model=model,
        input=[
            {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
            {"role": "user", "content": [
                {"type": "input_text", "text": user_prompt},
                {"type": "input_image", "image_url": data_url},
            ]},
        ],
        temperature=0.0,
        max_output_tokens=600,
    )

    raw = (resp.output_text or "").strip()
    js = extract_json_str(raw)
    if not js:
        return {"text": "", "type": "Other"}

    try:
        parsed = json.loads(js)
    except Exception:
        return {"text": "", "type": "Other"}

    text = (parsed.get("text") or "").strip()
    typ = (parsed.get("type") or "Other").strip()
    if typ not in categories_en:
        typ = "Other"

    return {"text": text, "type": typ}
