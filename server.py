#!/usr/bin/env python3
"""
Nuvio Local Provider — serveur API

Rôle :
  1. Reçoit les requêtes du provider Nuvio (TMDB ID, film ou série, saison/épisode)
  2. Transmet l'identifiant TMDB au téléchargeur, qui résout ensuite la source
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
import subprocess
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

from configuration import BASE_DIR, LIBRARY_PATH, PROVIDER_TEMPLATE, load_config
from downloader import hls_header_sets, hls_master_variants
from metadata import tmdb_lookup

cfg = load_config()
# Compatibilité descendante: si seule l'ancienne clé existe, on la reprend.
if "download_file_before" not in cfg:
    cfg["download_file_before"] = bool(cfg.get("download_file", False))
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

    def update(self, key: str, **fields: Any) -> None:
        entry = self.items.get(key)
        if entry:
            entry.update(fields)
            self._save()


library = Library(LIBRARY_PATH)


# --------------------------------------------------------------------------- #
# Pont vers ta fonction extract_video_stream() ou équivalent
# --------------------------------------------------------------------------- #


def _load_downloader():
    sys.path.insert(0, str(BASE_DIR))
    module = importlib.import_module(cfg["downloader"]["module"])
    return getattr(module, cfg["downloader"]["function"])


def _run_download(
    tmdb_id: str,
    ctype: str,
    season: int | None,
    episode: int | None,
    *,
    stream_only: bool,
) -> Any:
    """Exécuté dans un thread dédié : on y crée sa propre boucle asyncio,
    exactement comme un asyncio.run(extract_video_stream(...))."""
    fn = _load_downloader()
    kwargs: dict[str, Any] = {
        "provider": cfg["stream_provider"],
        "tmdb_id": tmdb_id,
        "content_type": ctype,
        "season": season,
        "episode": episode,
        "output_dir": DOWNLOAD_DIR,
        "stream_only": stream_only,
    }
    if ctype == "tv":
        kwargs.update(season=season, episode=episode)
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
    def __init__(self, job_key: str, content_key: str, mode: str):
        self.job_key = job_key
        self.content_key = content_key
        self.mode = mode  # "hls" | "download"
        self.status = "queued"  # queued | downloading | ready | failed
        self.error: str | None = None
        self.source_url: str | None = None
        self.quality: str | None = None
        self.media: dict[str, Any] = {}
        self.filename: str | None = None
        self.entry: dict | None = None
        self.created = time.time()
        self.finished: float | None = None
        self.event = asyncio.Event()
        self.task: asyncio.Task | None = None


jobs: dict[str, Job] = {}
_semaphore: asyncio.Semaphore | None = None
hls_sessions: dict[str, dict[str, Any]] = {}


def _extract_playlist_url(result: Any) -> str | None:
    if isinstance(result, str) and result.startswith(("http://", "https://")):
        return result
    if isinstance(result, dict):
        for key in ("hls_url", "playlist_url", "url", "source_url"):
            value = result.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                return value
        for value in result.values():
            found = _extract_playlist_url(value)
            if found:
                return found
    if isinstance(result, (list, tuple, set)):
        for value in result:
            found = _extract_playlist_url(value)
            if found:
                return found
    return None


_HLS_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"


def _hls_request_headers(referer: str | None, origin: str | None) -> dict[str, str]:
    headers = {"User-Agent": _HLS_USER_AGENT, "Accept": "*/*"}
    if referer:
        headers["Referer"] = referer
    if origin:
        headers["Origin"] = origin
    return headers


def _same_site(host_a: str, host_b: str) -> bool:
    """Vrai si les deux hôtes partagent leur domaine (ex. iad-01.x.cfd / iad-02.x.cfd)."""
    return host_a.split(".")[-2:] == host_b.split(".")[-2:]


async def _fetch_hls_playlist(
    client: httpx.AsyncClient, url: str, header_sets: list[tuple[str | None, str | None]]
) -> httpx.Response:
    """GET avec les jeux referer/origin successifs tant que le serveur répond 403.
    Suit les redirections (le proxy m3u8 peut répondre 307)."""
    response: httpx.Response | None = None
    for referer, origin in header_sets:
        response = await client.get(
            url, headers=_hls_request_headers(referer, origin), follow_redirects=True
        )
        if response.status_code != 403:
            break
    assert response is not None
    return response


def _hls_codec_labels(codecs: str) -> tuple[str | None, list[str]]:
    """Convertit un attribut CODECS HLS ("avc1.640028,mp4a.40.2") en libellés lisibles."""
    video: str | None = None
    audio: list[str] = []
    for codec in (c.strip().lower() for c in codecs.split(",") if c.strip()):
        if codec.startswith("avc"):
            video = "H.264"
        elif codec.startswith(("hvc1", "hev1")):
            video = "H.265"
        elif codec.startswith("av01"):
            video = "AV1"
        elif codec.startswith("vp09"):
            video = "VP9"
        elif codec.startswith("mp4a"):
            audio.append("AAC")
        elif codec.startswith("ac-3"):
            audio.append("AC3")
        elif codec.startswith("ec-3"):
            audio.append("E-AC3")
        elif codec.startswith("opus"):
            audio.append("Opus")
        elif codec.startswith("mp3"):
            audio.append("MP3")
    return video, list(dict.fromkeys(audio))


async def _probe_hls(playlist_url: str) -> dict[str, Any]:
    """Récupère qualité, codecs, débit, qualités disponibles et durée d'un flux HLS."""
    info: dict[str, Any] = {"quality": detect_hls_quality(playlist_url)}
    variants: list[dict] = []
    header_sets = hls_header_sets(playlist_url, cfg["stream_provider"])
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=False) as client:
            master = await _fetch_hls_playlist(client, playlist_url, header_sets)
            if not master.is_success:
                return info
            text = master.text
            info["quality"] = detect_hls_quality(playlist_url, text)

            media_text = text
            variants = hls_master_variants(text, playlist_url)
            if variants:
                best = variants[0]
                video, audio = _hls_codec_labels(best["codecs"])
                info["video_codec"] = video
                info["audio"] = audio
                if best["bandwidth"]:
                    info["bitrate"] = f"{best['bandwidth'] / 1_000_000:.1f} Mb/s"
                heights = sorted({v["height"] for v in variants if v["height"]}, reverse=True)
                info["qualities"] = ["4K" if h >= 2000 else f"{h}p" for h in heights]
                media = await _fetch_hls_playlist(client, best["url"], header_sets)
                media_text = media.text if media.is_success else ""

            durations = re.findall(r"#EXTINF:([\d.]+)", media_text)
            if durations:
                info["duration"] = int(sum(float(d) for d in durations))
    except httpx.HTTPError as exc:
        log.warning("Infos HLS indisponibles pour %s : %s", playlist_url, exc)

    # Les masters Atlantic n'indiquent pas CODECS : on laisse ffprobe lire l'init
    # segment de la meilleure variante pour obtenir codecs audio/vidéo.
    if not info.get("video_codec") and variants:
        for referer, origin in header_sets:
            blob = (f"Referer: {referer}\r\n" if referer else "") + (
                f"Origin: {origin}\r\n" if origin else ""
            )
            probed = await asyncio.to_thread(
                probe_media,
                variants[0]["url"],
                [
                    "-extension_picky", "0",
                    "-user_agent", _HLS_USER_AGENT,
                    *(["-headers", blob] if blob else []),
                ],
            )
            if probed:
                info["video_codec"] = probed.get("video_codec")
                info["audio"] = probed.get("audio", [])
                info["duration"] = info.get("duration") or probed.get("duration")
                break
    return info


async def _run_job(
    job: Job,
    tmdb_id: str,
    ctype: str,
    season: int | None,
    episode: int | None,
    title: str,
):
    assert _semaphore is not None
    async with _semaphore:
        job.status = "downloading"
        started = time.time()
        before = await asyncio.to_thread(snapshot_downloads)
        log.info("▶️  Job %s : %s (%s)", job.mode, title, job.content_key)
        try:
            result = await asyncio.to_thread(
                _run_download,
                tmdb_id,
                ctype,
                season,
                episode,
                stream_only=(job.mode == "hls"),
            )

            if job.mode == "hls":
                playlist_url = _extract_playlist_url(result)
                if not playlist_url:
                    raise RuntimeError("Capture HLS terminée sans URL de playlist")
                job.source_url = playlist_url
                result_quality = result.get("quality") if isinstance(result, dict) else None
                job.media = await _probe_hls(playlist_url)
                job.quality = result_quality or job.media["quality"]
                if isinstance(result, dict) and isinstance(result.get("filename"), str):
                    filename = result["filename"].strip()
                    if filename:
                        job.filename = filename
                if not job.filename:
                    job.filename = _stream_filename(title, ".m3u8")
                job.status = "ready"
                log.info("⚡ Flux HLS prêt : %s", title)
                return

            path = await asyncio.to_thread(locate_file, result, before, started)
            if path is None:
                raise RuntimeError(
                    "Téléchargement terminé mais aucun fichier mp4/mkv trouvé. "
                    "Vérifie 'download_dir' dans config.json ou fais retourner le chemin par ta fonction."
                )
            job.entry = library.put(job.content_key, path, title)
            await ensure_entry_media(job.entry)
            result_quality = result.get("quality") if isinstance(result, dict) else None
            result_filename = result.get("filename") if isinstance(result, dict) else None
            job.filename = result_filename if isinstance(result_filename, str) and result_filename.strip() else path.name
            job.quality = result_quality or detect_quality(path.name)
            job.status = "ready"
            log.info("✅ Fichier prêt : %s -> %s", title, path)
        except Exception as e:
            job.status = "failed"
            job.error = f"{type(e).__name__}: {e}"
            log.exception("❌ Échec du job %s pour %s", job.mode, job.content_key)
        finally:
            job.finished = time.time()
            job.event.set()


def ensure_job(
    content_key: str,
    mode: str,
    tmdb_id: str,
    ctype: str,
    season: int | None,
    episode: int | None,
    title: str,
) -> Job:
    job_key = f"{mode}:{content_key}"
    job = jobs.get(job_key)
    if job:
        if job.status in ("queued", "downloading"):
            return job
        if job.status == "failed" and time.time() - (job.finished or 0) < cfg["retry_cooldown_seconds"]:
            return job
    job = Job(job_key=job_key, content_key=content_key, mode=mode)
    jobs[job_key] = job
    job.task = asyncio.create_task(_run_job(job, tmdb_id, ctype, season, episode, title))
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


_VIDEO_CODEC_NAMES = {
    "h264": "H.264", "hevc": "H.265", "av1": "AV1", "vp9": "VP9", "mpeg4": "MPEG-4",
}
_AUDIO_CODEC_NAMES = {
    "aac": "AAC", "ac3": "AC3", "eac3": "E-AC3", "opus": "Opus", "mp3": "MP3",
    "flac": "FLAC", "dts": "DTS", "truehd": "TrueHD",
}
_CHANNEL_LAYOUTS = {1: "1.0", 2: "2.0", 6: "5.1", 8: "7.1"}


def probe_media(target: Path | str, input_args: list[str] | None = None) -> dict[str, Any]:
    """Analyse un fichier ou une URL avec ffprobe (résolution, codecs, durée).
    Retourne {} en cas d'échec."""
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "error", *(input_args or []), "-print_format", "json",
             "-show_streams", "-show_format", str(target)],
            capture_output=True, text=True, timeout=60, check=True,
        )
        data = json.loads(proc.stdout)
    except Exception as exc:  # noqa: BLE001
        log.warning("ffprobe impossible sur %s : %s", str(target)[:120], exc)
        return {}

    streams = data.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    info: dict[str, Any] = {}
    if video:
        width, height = int(video.get("width") or 0), int(video.get("height") or 0)
        # Les films en cinémascope (1920x804) restent du "1080p".
        effective = max(height, round(width * 9 / 16))
        info["width"], info["height"] = width, height
        info["quality"] = "4K" if effective >= 2000 else f"{effective}p" if effective else None
        codec = video.get("codec_name", "")
        info["video_codec"] = _VIDEO_CODEC_NAMES.get(codec, codec.upper())
    audio = []
    for s in streams:
        if s.get("codec_type") != "audio":
            continue
        codec = s.get("codec_name", "")
        label = _AUDIO_CODEC_NAMES.get(codec, codec.upper())
        layout = _CHANNEL_LAYOUTS.get(int(s.get("channels") or 0))
        label = f"{label} {layout}" if layout else label
        if label and label not in audio:
            audio.append(label)
    info["audio"] = audio
    try:
        info["duration"] = int(float(data.get("format", {}).get("duration", 0)))
    except (TypeError, ValueError):
        info["duration"] = 0
    return info


async def ensure_entry_media(entry: dict) -> None:
    """Analyse le fichier une seule fois et mémorise le résultat dans la bibliothèque."""
    if "media" in entry:
        return
    info = await asyncio.to_thread(probe_media, Path(entry["path"]))
    if info:
        entry["media"] = info
        library.update(entry["key"], media=info)


def human_duration(seconds: int) -> str:
    hours, rest = divmod(int(seconds), 3600)
    minutes = rest // 60
    return f"{hours}h{minutes:02d}" if hours else f"{minutes} min"


def build_download_stream(entry: dict, request: Request) -> list[dict]:
    path = Path(entry["path"])
    media = entry.get("media") or {}
    quality = media.get("quality") or detect_quality(path.name)
    size = human_size(entry["size"])
    url = f"{base_url(request)}/media/{entry['id']}/{quote(path.name)}"
    if cfg["api_key"]:
        url += f"?key={quote(cfg['api_key'])}"

    audio = " + ".join(media.get("audio") or [])
    codecs = " / ".join(p for p in (media.get("video_codec"), audio) if p)
    duration = human_duration(media["duration"]) if media.get("duration") else ""
    fmt = path.suffix.lstrip(".").upper()
    details = [quality, codecs, duration]
    return [
        {
            "name": cfg["provider"]["name"],
            "title": " • ".join(p for p in [entry["title"], *details, size, fmt] if p),
            "url": url,
            "quality": " • ".join(p for p in [path.name, *details] if p),
            "size": size,
            "filename": path.name,
            "format": path.suffix.lstrip(".").lower(),
            "resolution": quality,
            "video_codec": media.get("video_codec"),
            "audio": media.get("audio"),
            "duration": media.get("duration"),
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
        f"{job.job_key}:{job.source_url}".encode()
    ).hexdigest()[:24]
    hls_sessions[session_id] = {
        "url": job.source_url,
        "created": time.time(),
        "last_access": time.time(),
        "allowed_hosts": {urlsplit(job.source_url).hostname},
        # Jeux (referer, origin) à essayer ; celui qui réussit est mémorisé par hôte.
        "header_sets": hls_header_sets(job.source_url, cfg["stream_provider"]),
        "host_header_index": {},
    }
    url = f"{base_url(request)}/hls/{session_id}/{_encode_hls_resource(job.source_url)}"
    if cfg["api_key"]:
        url += f"?key={quote(cfg['api_key'])}"
    media = job.media or {}
    filename = job.filename or _stream_filename(title, ".m3u8")
    quality = job.quality or detect_hls_quality(job.source_url)
    audio = " + ".join(media.get("audio") or [])
    codecs = " / ".join(p for p in (media.get("video_codec"), audio) if p)
    duration = human_duration(media["duration"]) if media.get("duration") else ""
    details = [quality, codecs, media.get("bitrate", ""), duration]
    available = media.get("qualities") or []
    return [{
        "name": cfg["provider"]["name"],
        "title": " • ".join(p for p in [title, *details, "HLS"] if p),
        "url": url,
        "quality": " • ".join(p for p in [filename, *details] if p),
        "filename": filename,
        "format": "hls",
        "resolution": quality,
        "video_codec": media.get("video_codec"),
        "audio": media.get("audio"),
        "bitrate": media.get("bitrate"),
        "duration": media.get("duration"),
        "available_qualities": available,
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
    streams_endpoint = "/api/streams/download" if cfg.get("download_file_before", False) else "/api/streams/hls"
    replacements = {
        "__BASE_URL__": json.dumps(base_url(request)),
        "__API_KEY__": json.dumps(cfg["api_key"]),
        "__STREAMS_ENDPOINT__": json.dumps(streams_endpoint),
        "__MAX_WAIT_MS__": str(int(p["wait_seconds"]) * 1000),
        "__POLL_SECONDS__": str(int(p["poll_seconds"])),
    }
    for k, v in replacements.items():
        code = code.replace(k, v)
    return Response(code, media_type="application/javascript; charset=utf-8")


# ---- API principale -------------------------------------------------------- #


async def _resolve_stream_request(
    tmdb_id: str | None,
    imdb_id: str | None,
    media_type: str,
    season: int | None,
    episode: int | None,
) -> tuple[str, int | None, int | None, str, str, str]:
    ctype = "tv" if media_type in ("tv", "series", "show") else "movie"
    if ctype == "tv" and (season is None or episode is None):
        raise HTTPException(400, "season et episode sont requis pour type=tv")

    if imdb_id:
        meta = {"imdb_id": imdb_id, "title": imdb_id}
    elif tmdb_id and tmdb_id.startswith("tt"):
        meta = {"imdb_id": tmdb_id, "title": tmdb_id}
    elif tmdb_id:
        meta = await tmdb_lookup(tmdb_id, ctype, cfg["tmdb_api_key"], cfg["tmdb_language"])
    else:
        raise HTTPException(400, "tmdb_id ou imdb_id requis")

    source_id = tmdb_id or imdb_id
    if source_id is None:
        raise HTTPException(400, "tmdb_id requis pour le téléchargeur")

    title = meta["title"]
    if ctype == "tv":
        title = f"{title} S{season:02d}E{episode:02d}"
    content_key = make_key(meta["imdb_id"], ctype, season, episode)
    return ctype, season, episode, title, content_key, source_id


async def _await_job(job: Job, wait: int) -> None:
    if job.status in ("queued", "downloading") and wait > 0:
        try:
            await asyncio.wait_for(job.event.wait(), timeout=min(wait, 120))
        except TimeoutError:
            pass


def _stream_response(
    *,
    mode: str,
    status: str,
    content_key: str,
    title: str,
    request: Request,
    error: str | None = None,
    entry: dict | None = None,
    job: Job | None = None,
) -> dict[str, Any]:
    streams: list[dict] = []
    if mode == "download" and entry:
        streams = build_download_stream(entry, request)
    elif mode == "hls" and job and job.source_url:
        streams = build_hls_stream(job, request, title)
    return {
        "mode": mode,
        "status": status,
        "key": content_key,
        "title": title,
        "error": error,
        "streams": streams,
    }


@app.get("/api/streams/hls")
async def streams_hls(
    request: Request,
    tmdb_id: str | None = None,
    imdb_id: str | None = None,
    media_type: str = Query("movie", alias="type"),
    season: int | None = None,
    episode: int | None = None,
    wait: int = 0,
):
    check_key(request)
    ctype, season, episode, title, content_key, source_id = await _resolve_stream_request(
        tmdb_id, imdb_id, media_type, season, episode
    )

    job = ensure_job(content_key, "hls", source_id, ctype, season, episode, title)
    await _await_job(job, wait)

    if job.status == "failed":
        return _stream_response(
            mode="hls",
            status="failed",
            content_key=content_key,
            title=title,
            request=request,
            error=job.error,
            job=job,
        )
    return _stream_response(
        mode="hls",
        status=job.status,
        content_key=content_key,
        title=title,
        request=request,
        job=job,
    )


@app.get("/api/streams/download")
async def streams_download(
    request: Request,
    tmdb_id: str | None = None,
    imdb_id: str | None = None,
    media_type: str = Query("movie", alias="type"),
    season: int | None = None,
    episode: int | None = None,
    wait: int = 0,
):
    check_key(request)
    ctype, season, episode, title, content_key, source_id = await _resolve_stream_request(
        tmdb_id, imdb_id, media_type, season, episode
    )

    entry = library.get(content_key)
    if entry:
        await ensure_entry_media(entry)
        return _stream_response(
            mode="download",
            status="ready",
            content_key=content_key,
            title=title,
            request=request,
            entry=entry,
        )

    job = ensure_job(content_key, "download", source_id, ctype, season, episode, title)
    await _await_job(job, wait)

    entry = library.get(content_key)
    if entry:
        await ensure_entry_media(entry)
        return _stream_response(
            mode="download",
            status="ready",
            content_key=content_key,
            title=title,
            request=request,
            entry=entry,
            job=job,
        )
    if job.status == "failed":
        return _stream_response(
            mode="download",
            status="failed",
            content_key=content_key,
            title=title,
            request=request,
            error=job.error,
            job=job,
        )
    return _stream_response(
        mode="download",
        status=job.status,
        content_key=content_key,
        title=title,
        request=request,
        job=job,
    )


@app.get("/api/streams")
async def streams_default(
    request: Request,
    tmdb_id: str | None = None,
    imdb_id: str | None = None,
    media_type: str = Query("movie", alias="type"),
    season: int | None = None,
    episode: int | None = None,
    wait: int = 0,
):
    if cfg.get("download_file_before", False):
        return await streams_download(request, tmdb_id, imdb_id, media_type, season, episode, wait)
    return await streams_hls(request, tmdb_id, imdb_id, media_type, season, episode, wait)


@app.get("/api/library")
async def api_library(request: Request):
    check_key(request)
    return list(library.items.values())


@app.get("/api/jobs")
async def api_jobs(request: Request):
    check_key(request)
    return [
        {
            "job_key": j.job_key,
            "key": j.content_key,
            "mode": j.mode,
            "status": j.status,
            "error": j.error,
            "created": int(j.created),
        }
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
    if parsed.hostname not in session["allowed_hosts"]:
        raise HTTPException(403, "Destination HLS non autorisée")

    session["last_access"] = time.time()
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0, connect=10.0),
            follow_redirects=False,
        ) as client:
            header_sets = session["header_sets"]
            host_index = session["host_header_index"]
            first = host_index.get(parsed.hostname, 0)
            order = [first] + [i for i in range(len(header_sets)) if i != first]
            upstream = None
            for index in order:
                referer, origin = header_sets[index]
                upstream = await client.get(upstream_url, headers=_hls_request_headers(referer, origin))
                if upstream.status_code != 403:
                    host_index[parsed.hostname] = index
                    break

            # Les proxys m3u8 renvoient parfois un 307 vers un nœud voisin du même
            # domaine : on le suit (3 sauts max, hôtes du même site uniquement).
            for _ in range(3):
                location = upstream.headers.get("location")
                if not (300 <= upstream.status_code < 400 and location):
                    break
                target = urljoin(upstream_url, location)
                target_parts = urlsplit(target)
                if (
                    target_parts.scheme != "https"
                    or not target_parts.hostname
                    or not (
                        target_parts.hostname in session["allowed_hosts"]
                        or _same_site(target_parts.hostname, parsed.hostname)
                    )
                ):
                    break
                session["allowed_hosts"].add(target_parts.hostname)
                upstream_url, parsed = target, target_parts
                referer, origin = header_sets[host_index.get(parsed.hostname, order[0])]
                upstream = await client.get(upstream_url, headers=_hls_request_headers(referer, origin))
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

    # Atlantic utilise plusieurs CDN/domaines pour les sous-playlists et les
    # segments. Ils deviennent autorisés uniquement après apparition dans une
    # playlist provenant d'un domaine déjà autorisé.
    playlist_uris = re.findall(r'URI="([^"]+)"', text)
    playlist_uris.extend(
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    playlist_hosts = set()
    for uri in playlist_uris:
        target = urlsplit(urljoin(upstream_url, uri))
        if target.scheme == "https" and target.hostname:
            playlist_hosts.add(target.hostname)
    session["allowed_hosts"].update(playlist_hosts)

    # URI= attributes cover keys/maps, while plain URI lines cover segments
    # and child playlists.
    def rewrite_uri_attribute(match: re.Match[str]) -> str:
        target = urljoin(upstream_url, match.group(2))
        target_parts = urlsplit(target)
        if (
            target_parts.hostname not in session["allowed_hosts"]
            or target_parts.scheme != "https"
        ):
            raise HTTPException(403, "URI HLS externe non autorisée")
        return f"{match.group(1)}{_hls_proxy_url(request, session_id, target)}{match.group(3)}"

    text = re.sub(r'(URI=")([^"]+)(")', rewrite_uri_attribute, text)
    lines = []
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            target = urljoin(upstream_url, stripped)
            target_parts = urlsplit(target)
            if (
                target_parts.hostname not in session["allowed_hosts"]
                or target_parts.scheme != "https"
            ):
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
