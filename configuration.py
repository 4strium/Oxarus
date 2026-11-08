import json
import os
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get("NUVIO_CONFIG", BASE_DIR / "config.json"))
PROVIDER_TEMPLATE = BASE_DIR / "provider" / "localserver.template.js"
LIBRARY_PATH = BASE_DIR / "library.json"

DEFAULTS: dict[str, Any] = {
    "host": "0.0.0.0",
    "port": 8000,
    "public_url": "",
    "cors_origins": ["*"],
    "api_key": "",
    "tmdb_api_key": "",
    "tmdb_language": "fr-FR",
    "download_dir": "./downloads",
    "downloader": {"module": "downloader", "function": "extract_video_stream"},
    "max_concurrent_downloads": 1,
    "download_file_before": False,
    "hls_session_ttl_seconds": 7200,
    "retry_cooldown_seconds": 300,
    "video_extensions": [".mp4", ".mkv"],
    "manifest_format": "scrapers",
    "provider": {
        "id": "local-server",
        "name": "My Local Server",
        "description": "Streams files downloaded by my personal server.",
        "version": "1.0.0",
        "author": "Me",
        "content_language": ["fr", "en"],
        "wait_seconds": 0,  # 0 = wait for the download to finish, with no limit
        "poll_seconds": 20,
    },
}

def deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise SystemExit(f"Configuration file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return deep_merge(DEFAULTS, json.load(f))