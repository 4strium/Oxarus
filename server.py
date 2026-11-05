#!/usr/bin/env python3
"""
Nuvio Local Provider — serveur API

Rôle :
  1. Reçoit les requêtes du provider Nuvio (TMDB ID, film ou série, saison/épisode)
  2. Convertit TMDB -> IMDb (ta fonction Python attend un imdb_id)
  3. Publie immédiatement le flux HLS capturé, ou télécharge un fichier si le
     mode direct est désactivé
  4. Proxyfie les playlists/segments HLS et sert les fichiers mp4/mkv en HTTP
     (Range supporté, donc seek OK)
  5. Sert /manifest.json et /providers/localserver.js générés depuis config.json

Lancement :  python server.py
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import importlib
import json
import logging
import os
import re
import socket
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlsplit

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
    Response,
    StreamingResponse,
)

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
    "downloader": {"module": "downloader", "function": "extract_and_download_video"},
    "max_concurrent_downloads": 1,
    "download_file": False,
    "direct_hls": True,
    "hls_session_ttl_seconds": 7200,
    "retry_cooldown_seconds": 300,
    "video_extensions": [".mp4", ".mkv"],
    "manifest_format": "scrapers",
    "provider": {
        "id": "local-server",
        "name": "Mon Serveur Local",
        "description": "Streame les fichiers téléchargés par mon serveur personnel.",
        "version": "1.0.0",
        "author": "Moi",
        "content_language": ["fr", "en"],
        "wait_seconds": 90,
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
        raise SystemExit(f"Fichier de config introuvable : {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return deep_merge(DEFAULTS, json.load(f))


cfg = load_config()
os.chdir(BASE_DIR)  # les chemins relatifs partent du dossier du projet

DOWNLOAD_DIR = (BASE_DIR / cfg["download_dir"]).resolve()
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
VIDEO_EXTS = {e.lower() for e in cfg["video_extensions"]}
PARTIAL_SUFFIXES = {".part", ".tmp", ".crdownload", ".download", ".aria2", ".ytdl"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nuvio-local")

# --------------------------------------------------------------------------- #
# Bibliothèque (index persistant clé -> fichier)
# --------------------------------------------------------------------------- #


class Library:
    def __init__(self, path: Path):
        self.path = path
        self.items: dict[str, dict] = {}
        if path.exists():
            try:
                self.items = json.loads(path.read_text(encoding="utf-8"))
            except Exception as e:  # noqa: BLE001
                log.warning("library.json illisible (%s), on repart de zéro", e)

    def _save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.items, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)

    def get(self, key: str) -> dict | None:
        entry = self.items.get(key)
        if entry and not Path(entry["path"]).is_file():
            log.info("Fichier disparu, retiré de la bibliothèque : %s", entry["path"])
            del self.items[key]
            self._save()
            return None
        return entry

    def by_id(self, file_id: str) -> dict | None:
        for entry in self.items.values():
            if entry["id"] == file_id and Path(entry["path"]).is_file():
                return entry
        return None

    def put(self, key: str, path: Path, title: str) -> dict:
        entry = {
            "id": hashlib.sha1(key.encode()).hexdigest()[:16],
            "key": key,
            "path": str(path),
            "title": title,
            "size": path.stat().st_size,
            "added": int(time.time()),
        }
        self.items[key] = entry
        self._save()
        return entry


library = Library(LIBRARY_PATH)

# --------------------------------------------------------------------------- #
# TMDB -> IMDb
# --------------------------------------------------------------------------- #

_tmdb_cache: dict[str, dict] = {}


async def tmdb_lookup(tmdb_id: str, ctype: str) -> dict:
    """Retourne {"imdb_id", "title"} pour un ID TMDB (film ou série)."""
    cache_key = f"{ctype}:{tmdb_id}"
    if cache_key in _tmdb_cache:
        return _tmdb_cache[cache_key]

    api_key = cfg["tmdb_api_key"]
    if not api_key or api_key.startswith("VOTRE"):
        raise HTTPException(500, "tmdb_api_key manquante dans config.json")

    headers: dict[str, str] = {}
    params = {"append_to_response": "external_ids", "language": cfg["tmdb_language"]}
    if len(api_key) > 40:  # jeton v4 (Bearer)
        headers["Authorization"] = f"Bearer {api_key}"
    else:  # clé v3
        params["api_key"] = api_key

    url = f"https://api.themoviedb.org/3/{'movie' if ctype == 'movie' else 'tv'}/{tmdb_id}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params=params, headers=headers)
    except httpx.HTTPError as e:
        raise HTTPException(502, f"TMDB injoignable : {e}") from e

    if r.status_code == 404:
        raise HTTPException(404, f"TMDB {ctype} {tmdb_id} introuvable")
    if r.status_code >= 400:
        raise HTTPException(502, f"TMDB a répondu {r.status_code}")

    data = r.json()
    imdb_id = data.get("imdb_id") or (data.get("external_ids") or {}).get("imdb_id")
    if not imdb_id:
        raise HTTPException(404, f"Aucun ID IMDb pour TMDB {ctype} {tmdb_id}")

    title = data.get("title") or data.get("name") or imdb_id
    date = data.get("release_date") or data.get("first_air_date") or ""
    if date[:4].isdigit():
        title = f"{title} ({date[:4]})"

    result = {"imdb_id": imdb_id, "title": title}
    _tmdb_cache[cache_key] = result
    return result


# --------------------------------------------------------------------------- #
# Pont vers ta fonction extract_and_download_video() ou équivalent
# --------------------------------------------------------------------------- #


def _load_downloader():
    sys.path.insert(0, str(BASE_DIR))
    module = importlib.import_module(cfg["downloader"]["module"])
    return getattr(module, cfg["downloader"]["function"])


def _run_download(imdb_id: str, ctype: str, season: int | None, episode: int | None) -> Any:
    """Exécuté dans un thread dédié : on y crée sa propre boucle asyncio,
    exactement comme ton asyncio.run(extract_and_download_video(...))."""
    fn = _load_downloader()
    kwargs: dict[str, Any] = {
        "imdb_id": imdb_id,
        "content_type": ctype,
        "output_dir": DOWNLOAD_DIR,
    }
    if ctype == "tv":
        kwargs.update(season=season, episode=episode)
    # `download_file` est le réglage explicite : quand il est désactivé,
    # seule la capture HLS est effectuée et aucun fichier n'est créé.
    kwargs["stream_only"] = not cfg.get("download_file", False)
    result = fn(**kwargs)
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)
    return result


def _is_video(p: Path) -> bool:
    return p.suffix.lower() in VIDEO_EXTS and p.suffix.lower() not in PARTIAL_SUFFIXES


def snapshot_downloads() -> dict[str, tuple[int, float]]:
    snap: dict[str, tuple[int, float]] = {}
    for p in DOWNLOAD_DIR.rglob("*"):
        if p.is_file() and _is_video(p):
            st = p.stat()
            snap[str(p)] = (st.st_size, st.st_mtime)
    return snap


def _paths_from_result(result: Any) -> list[Path]:
    """Extrait les chemins de fichiers éventuels retournés par le téléchargeur."""
    if isinstance(result, (str, Path)):
        return [Path(result)]
    if isinstance(result, dict):
        return [p for v in result.values() for p in _paths_from_result(v)]
    if isinstance(result, (list, tuple, set)):
        return [p for v in result for p in _paths_from_result(v)]
    return []


def locate_file(result: Any, before: dict, started: float) -> Path | None:
    # 1) la fonction retourne le chemin (ou un dict/tuple le contenant)
    for p in _paths_from_result(result):
        try:
            if p.is_file() and _is_video(p):
                return p.resolve()
        except OSError:
            continue

    # 2) sinon : fichier vidéo nouveau/modifié dans download_dir, le plus gros
    candidates = []
    for path_str, (size, mtime) in snapshot_downloads().items():
        if size > 0 and (path_str not in before or before[path_str] != (size, mtime)) and mtime >= started - 2:
            candidates.append((size, Path(path_str)))
    if candidates:
        return max(candidates)[1].resolve()
    return None


# --------------------------------------------------------------------------- #
# Gestion des jobs de téléchargement
# --------------------------------------------------------------------------- #


class Job:
    def __init__(self, key: str):
        self.key = key
        self.status = "queued"  # queued | downloading | ready | failed
        self.error: str | None = None
        self.source_url: str | None = None
        self.quality: str | None = None
        self.created = time.time()
        self.finished: float | None = None
        self.event = asyncio.Event()
        self.task: asyncio.Task | None = None


jobs: dict[str, Job] = {}
_semaphore: asyncio.Semaphore | None = None
hls_sessions: dict[str, dict[str, Any]] = {}


async def _run_job(job: Job, imdb_id: str, ctype: str, season: int | None, episode: int | None, title: str):
    assert _semaphore is not None
    async with _semaphore:
        job.status = "downloading"
        started = time.time()
        before = await asyncio.to_thread(snapshot_downloads)
        log.info("⬇️  Téléchargement : %s (%s)", title, job.key)
        try:
            result = await asyncio.to_thread(_run_download, imdb_id, ctype, season, episode)
            if isinstance(result, str) and result.startswith(("http://", "https://")):
                job.source_url = result
                job.quality = detect_hls_quality(result)
                try:
                    async with httpx.AsyncClient(timeout=5, follow_redirects=False) as client:
                        playlist_response = await client.get(result)
                    if playlist_response.is_success:
                        job.quality = detect_hls_quality(
                            result, playlist_response.text
                        )
                except httpx.HTTPError as exc:
                    log.warning("Qualité HLS indisponible pour %s : %s", title, exc)
                job.status = "ready"
                log.info("⚡ Flux HLS prêt : %s", title)
                return

            path = await asyncio.to_thread(locate_file, result, before, started)
            if path is None:
                raise RuntimeError(
                    "Téléchargement terminé mais aucun fichier mp4/mkv trouvé. "
                    "Vérifie 'download_dir' dans config.json ou fais retourner le chemin par ta fonction."
                )
            library.put(job.key, path, title)
            job.status = "ready"
            log.info("✅ Prêt : %s -> %s", title, path)
        except Exception as e:
            job.status = "failed"
            job.error = f"{type(e).__name__}: {e}"
            log.exception("❌ Échec du téléchargement de %s", job.key)
        finally:
            job.finished = time.time()
            job.event.set()


def ensure_job(key: str, imdb_id: str, ctype: str, season: int | None, episode: int | None, title: str) -> Job:
    job = jobs.get(key)
    if job:
        if job.status in ("queued", "downloading"):
            return job
        if job.status == "failed" and time.time() - (job.finished or 0) < cfg["retry_cooldown_seconds"]:
            return job
    job = Job(key)
    jobs[key] = job
    job.task = asyncio.create_task(_run_job(job, imdb_id, ctype, season, episode, title))
    return job


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def check_key(request: Request) -> None:
    expected = cfg["api_key"]
    if not expected:
        return
    given = request.headers.get("x-api-key") or request.query_params.get("key") or ""
    if not hmac.compare_digest(given, expected):
        raise HTTPException(401, "Clé API invalide")


def base_url(request: Request) -> str:
    return (cfg["public_url"] or str(request.base_url)).rstrip("/")


def human_size(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{n} B"


def detect_quality(filename: str) -> str:
    n = filename.lower()
    if re.search(r"2160p|\b4k\b|\buhd\b", n):
        return "4K"
    for q in ("1080p", "720p", "480p"):
        if q in n:
            return q
    return "Unknown"


def detect_hls_quality(url: str, playlist: str = "") -> str:
    source = f"{url}\n{playlist}".lower()
    resolutions = [int(height) for width, height in re.findall(
        r"(?:resolution=)?(\d{3,4})x(\d{3,4})", source
    ) if int(width) >= 320 and int(height) >= 240]
    if resolutions:
        height = max(resolutions)
        if height >= 2160:
            return "4K"
        return f"{height}p"
    if re.search(r"2160p|4k|uhd", source):
        return "4K"
    for quality in ("1080p", "720p", "480p"):
        if quality in source:
            return quality
    return "Auto"


def build_streams(entry: dict, request: Request) -> list[dict]:
    path = Path(entry["path"])
    quality = detect_quality(path.name)
    size = human_size(entry["size"])
    url = f"{base_url(request)}/media/{entry['id']}/{quote(path.name)}"
    if cfg["api_key"]:
        url += f"?key={quote(cfg['api_key'])}"
    return [
        {
            "name": cfg["provider"]["name"],
            "title": f"{entry['title']} • {quality} • {size} • {path.suffix.lstrip('.').upper()}",
            "url": url,
            "quality": quality,
            "size": size,
        }
    ]


def _encode_hls_resource(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")


def _stream_filename(title: str, extension: str = ".mp4") -> str:
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", title).strip(" .")
    return f"{filename or 'video'}{extension}"


def build_hls_stream(job: Job, request: Request, title: str) -> list[dict]:
    if not job.source_url:
        return []
    session_id = hashlib.sha256(
        f"{job.key}:{job.source_url}".encode()
    ).hexdigest()[:24]
    hls_sessions[session_id] = {
        "url": job.source_url,
        "created": time.time(),
        "last_access": time.time(),
        "host": urlsplit(job.source_url).hostname,
    }
    url = f"{base_url(request)}/hls/{session_id}/{_encode_hls_resource(job.source_url)}"
    if cfg["api_key"]:
        url += f"?key={quote(cfg['api_key'])}"
    return [{
        "name": cfg["provider"]["name"],
        "title": f"{title} • HLS",
        "url": url,
        "quality": job.quality or detect_hls_quality(job.source_url),
        "filename": _stream_filename(title, ".m3u8"),
        "format": "hls",
    }]


def _cleanup_hls_sessions() -> None:
    now = time.time()
    ttl = max(60, int(cfg["hls_session_ttl_seconds"]))
    for session_id, session in list(hls_sessions.items()):
        if now - session["last_access"] > ttl:
            del hls_sessions[session_id]


def _decode_hls_resource(resource: str) -> str:
    padded = resource + "=" * (-len(resource) % 4)
    try:
        return base64.urlsafe_b64decode(padded.encode()).decode()
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(400, "Ressource HLS invalide") from exc


def _hls_proxy_url(request: Request, session_id: str, upstream_url: str) -> str:
    url = f"{base_url(request)}/hls/{session_id}/{_encode_hls_resource(upstream_url)}"
    if cfg["api_key"]:
        url += f"?key={quote(cfg['api_key'])}"
    return url


def local_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("10.255.255.255", 1))
            return s.getsockname()[0]
    except OSError:
        return "localhost"


def make_key(imdb_id: str, ctype: str, season: int | None, episode: int | None) -> str:
    if ctype == "tv":
        return f"tv:{imdb_id}:s{season:02d}e{episode:02d}"
    return f"movie:{imdb_id}"


# --------------------------------------------------------------------------- #
# Application FastAPI
# --------------------------------------------------------------------------- #


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _semaphore
    _semaphore = asyncio.Semaphore(max(1, int(cfg["max_concurrent_downloads"])))
    yield


app = FastAPI(title="Nuvio Local Provider", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cfg["cors_origins"],
    allow_credentials=False,
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Accept-Ranges", "Content-Length", "Content-Range"],
)


@app.get("/", response_class=PlainTextResponse)
async def root(request: Request):
    b = base_url(request)
    return (
        "Nuvio Local Provider en ligne.\n\n"
        f"URL du manifest à ajouter dans Nuvio : {b}/manifest.json\n"
        f"Bibliothèque : {b}/api/library\n"
        f"Jobs : {b}/api/jobs\n"
    )


@app.get("/health")
async def health():
    return {"ok": True}


# ---- Manifest + provider générés depuis config.json ----------------------- #


@app.get("/manifest.json")
async def manifest(request: Request):
    p = cfg["provider"]
    entry = {
        "id": p["id"],
        "name": p["name"],
        "description": p["description"],
        "version": p["version"],
        "author": p["author"],
        "supportedTypes": ["movie", "tv"],
        "filename": "providers/localserver.js",
        "enabled": True,
        "logo": "",
        "contentLanguage": p["content_language"],
        "formats": [e.lstrip(".") for e in cfg["video_extensions"]],
        "limited": False,
        "disabledPlatforms": [],
        "supportsExternalPlayer": True,
    }
    if cfg["manifest_format"] == "scrapers":
        body: Any = {"name": p["name"], "version": p["version"], "scrapers": [entry]}
    else:
        body = [entry]
    return JSONResponse(body)


@app.get("/providers/localserver.js")
async def provider_js(request: Request):
    code = PROVIDER_TEMPLATE.read_text(encoding="utf-8")
    p = cfg["provider"]
    replacements = {
        "__BASE_URL__": json.dumps(base_url(request)),
        "__API_KEY__": json.dumps(cfg["api_key"]),
        "__MAX_WAIT_MS__": str(int(p["wait_seconds"]) * 1000),
        "__POLL_SECONDS__": str(int(p["poll_seconds"])),
    }
    for k, v in replacements.items():
        code = code.replace(k, v)
    return Response(code, media_type="application/javascript; charset=utf-8")


# ---- API principale -------------------------------------------------------- #


@app.get("/api/streams")
async def streams(
    request: Request,
    tmdb_id: str | None = None,
    imdb_id: str | None = None,
    media_type: str = Query("movie", alias="type"),
    season: int | None = None,
    episode: int | None = None,
    wait: int = 0,
):
    """
    Appelé par le provider Nuvio.
    - fichier en bibliothèque -> status "ready" + streams
    - sinon lance le téléchargement (une seule fois) et attend jusqu'à `wait` secondes
    """
    check_key(request)

    ctype = "tv" if media_type in ("tv", "series", "show") else "movie"
    if ctype == "tv" and (season is None or episode is None):
        raise HTTPException(400, "season et episode sont requis pour type=tv")

    # Résolution de l'IMDb ID + titre
    if imdb_id:
        meta = {"imdb_id": imdb_id, "title": imdb_id}
    elif tmdb_id and tmdb_id.startswith("tt"):
        meta = {"imdb_id": tmdb_id, "title": tmdb_id}
    elif tmdb_id:
        meta = await tmdb_lookup(tmdb_id, ctype)
    else:
        raise HTTPException(400, "tmdb_id ou imdb_id requis")

    title = meta["title"]
    if ctype == "tv":
        title = f"{title} S{season:02d}E{episode:02d}"
    key = make_key(meta["imdb_id"], ctype, season, episode)

    def response(
        status: str,
        error: str | None = None,
        entry: dict | None = None,
        job: Job | None = None,
    ):
        streams = build_streams(entry, request) if entry else []
        if not streams and job and job.source_url:
            streams = build_hls_stream(job, request, title)
        return {
            "status": status,
            "key": key,
            "title": title,
            "error": error,
            "streams": streams,
        }

    entry = library.get(key)
    if entry:
        return response("ready", entry=entry)

    job = ensure_job(key, meta["imdb_id"], ctype, season, episode, title)

    if job.status in ("queued", "downloading") and wait > 0:
        try:
            await asyncio.wait_for(job.event.wait(), timeout=min(wait, 120))
        except TimeoutError:
            pass

    entry = library.get(key)
    if entry:
        return response("ready", entry=entry, job=job)
    if job.status == "failed":
        return response("failed", error=job.error, job=job)
    return response(job.status, job=job)


@app.get("/api/library")
async def api_library(request: Request):
    check_key(request)
    return list(library.items.values())


@app.get("/api/jobs")
async def api_jobs(request: Request):
    check_key(request)
    return [
        {"key": j.key, "status": j.status, "error": j.error, "created": int(j.created)}
        for j in jobs.values()
    ]


# ---- Proxy HLS (playlist + segments) -------------------------------------- #


@app.get("/hls/{session_id}/{resource}")
async def hls(
    session_id: str,
    resource: str,
    request: Request,
):
    check_key(request)
    _cleanup_hls_sessions()
    session = hls_sessions.get(session_id)
    if not session:
        raise HTTPException(404, "Session HLS expirée")

    upstream_url = _decode_hls_resource(resource)
    parsed = urlsplit(upstream_url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise HTTPException(400, "Destination HLS invalide")
    if parsed.hostname != session["host"]:
        raise HTTPException(403, "Destination HLS non autorisée")

    session["last_access"] = time.time()
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0, connect=10.0),
            follow_redirects=False,
        ) as client:
            upstream = await client.get(
                upstream_url,
                headers={"User-Agent": "Oxarus-HLS-Proxy/1.0", "Accept": "*/*"},
            )
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"Source HLS injoignable : {exc}") from exc

    if upstream.status_code >= 400:
        return Response(
            upstream.content,
            status_code=upstream.status_code,
            headers={"Cache-Control": "no-store"},
        )
    if 300 <= upstream.status_code < 400:
        raise HTTPException(502, "Redirection HLS refusée")

    content_type = upstream.headers.get("content-type", "")
    is_playlist = (
        parsed.path.lower().endswith((".m3u8", ".m3u"))
        or "mpegurl" in content_type.lower()
        or upstream.content.startswith(b"#EXTM3U")
    )
    if not is_playlist:
        return Response(
            upstream.content,
            media_type=content_type or "application/octet-stream",
            headers={"Cache-Control": "no-store"},
        )

    text = upstream.content.decode("utf-8", errors="replace")

    # URI= attributes cover keys/maps, while plain URI lines cover segments
    # and child playlists.
    def rewrite_uri_attribute(match: re.Match[str]) -> str:
        target = urljoin(upstream_url, match.group(2))
        target_parts = urlsplit(target)
        if target_parts.hostname != session["host"] or target_parts.scheme != "https":
            raise HTTPException(403, "URI HLS externe non autorisée")
        return f"{match.group(1)}{_hls_proxy_url(request, session_id, target)}{match.group(3)}"

    text = re.sub(r'(URI=")([^"]+)(")', rewrite_uri_attribute, text)
    lines = []
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            target = urljoin(upstream_url, stripped)
            target_parts = urlsplit(target)
            if target_parts.hostname != session["host"] or target_parts.scheme != "https":
                raise HTTPException(403, "URI HLS externe non autorisée")
            line = line.replace(stripped, _hls_proxy_url(request, session_id, target), 1)
        lines.append(line)
    return Response(
        "".join(lines),
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-store"},
    )


# ---- Streaming du fichier (HTTP Range) ------------------------------------ #


def _iter_file(path: Path, start: int, length: int, chunk: int = 1024 * 1024):
    with open(path, "rb") as f:
        f.seek(start)
        remaining = length
        while remaining > 0:
            data = f.read(min(chunk, remaining))
            if not data:
                break
            remaining -= len(data)
            yield data


@app.api_route("/media/{file_id}/{filename:path}", methods=["GET", "HEAD"])
async def media(file_id: str, filename: str, request: Request):
    check_key(request)
    entry = library.by_id(file_id)
    if not entry:
        raise HTTPException(404, "Fichier inconnu")

    path = Path(entry["path"])
    size = path.stat().st_size
    ctype = "video/mp4" if path.suffix.lower() == ".mp4" else "video/x-matroska"

    start, end, status = 0, size - 1, 200
    headers = {"Accept-Ranges": "bytes"}

    range_header = request.headers.get("range")
    if range_header:
        m = re.fullmatch(r"bytes=(\d*)-(\d*)", range_header.strip())
        if not m or (m.group(1) == "" and m.group(2) == ""):
            return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
        s, e = m.groups()
        if s == "":  # suffixe : les N derniers octets
            start = max(size - int(e), 0)
        else:
            start = int(s)
            if e:
                end = min(int(e), size - 1)
        if start >= size or start > end:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    length = end - start + 1
    headers["Content-Length"] = str(length)

    if request.method == "HEAD":
        return Response(status_code=status, headers=headers, media_type=ctype)

    return StreamingResponse(
        _iter_file(path, start, length), status_code=status, headers=headers, media_type=ctype
    )


# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    ip = local_ip()
    shown = (cfg["public_url"] or f"http://{ip}:{cfg['port']}").rstrip("/")
    print(f"\n🚀 Serveur démarré          : {shown}/")
    print(f"📝 Manifest à ajouter Nuvio : {shown}/manifest.json")
    print(f"📂 Dossier de téléchargement: {DOWNLOAD_DIR}\n")
    uvicorn.run(app, host=cfg["host"], port=int(cfg["port"]), log_level="info")
