"""
gemini_client.py — thin REST client for Google's Gemini API.

Used to get an INDEPENDENT second opinion from a different model family than
Claude, so its errors are uncorrelated with the model's own reasoning. Two
consumers: analysis_report (nightly red-team of the calibration data) and
ask_model (the Statalizer Bot's own research read).

Auth: set GEMINI_API_KEY in the Railway env (get one free from Google AI Studio).
Model: defaults to gemini-2.5-flash (cheap/fast); set GEMINI_MODEL=gemini-2.5-pro
for the deeper red-team read. The key is sent as a header, never in the URL.

Degrades gracefully: if the key is missing or the call fails, returns a short
bracketed notice instead of raising, so callers can render it inline.
"""
import os
import json
import logging
import urllib.request

log = logging.getLogger(__name__)

_DEFAULT_MODEL = "gemini-2.5-flash"


def gemini_available() -> bool:
    """True if a GEMINI_API_KEY is configured."""
    return bool(os.environ.get("GEMINI_API_KEY", "").strip())


def call_gemini(system: str, user: str, max_tokens: int = 1400,
                temperature: float = 0.7, timeout: int = 40) -> str:
    """Send a system + user prompt to Gemini and return the text answer.
    Never raises — returns a bracketed notice on any failure."""
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        return ("[GEMINI_API_KEY not set — add it in Railway env vars "
                "(free key from Google AI Studio) to enable the Gemini second opinion.]")
    model = os.environ.get("GEMINI_MODEL", _DEFAULT_MODEL).strip() or _DEFAULT_MODEL
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload = {
        "system_instruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": temperature},
    }
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode(),
            headers={"x-goog-api-key": api_key, "content-type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read())
        cands = body.get("candidates") or []
        if not cands:
            # Surfaces safety blocks / empty responses instead of failing silently.
            reason = body.get("promptFeedback", {}).get("blockReason", "")
            return f"[Gemini returned no answer{(' — ' + reason) if reason else ''}.]"
        parts = cands[0].get("content", {}).get("parts", []) or []
        text = "".join(p.get("text", "") for p in parts).strip()
        return text or "[Gemini returned an empty answer.]"
    except Exception as e:
        log.warning(f"Gemini call failed ({model}): {e}")
        return f"[Gemini call failed: {e}]"
