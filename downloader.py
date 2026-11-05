import asyncio
import base64
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

from playwright.async_api import async_playwright

from configuration import load_config
from metadata import tmdb_lookup

# Domaines liés à la publicité et au pistage à bloquer
ADS_AND_TRACKING_DOMAINS = [
    "aliexpress", "histats", "adexchangerapid", "pubtrk", "alicdn",
    "google", "aidcgroup", "alilog", "gstatic", "googleapis", "click"
]

STREAMING_PROVIDERS = {
    "vsembed": {
        "base_url": "https://vsembed.ru/",
        "movie_url": "https://vsembed.ru/embed/movie/{}/",
        "meta_movie_url": "https://data.vidsrc.sh/api.php?type=movie&imdb={}",
        "tv_url": "https://vsembed.ru/embed/tv/{}?season={}&episode={}",
        "meta_tv_url": "https://data.vidsrc.sh/api.php?type=tv&imdb={}&season={}&episode={}",
        "url_markers": [".m3u8", "token="],
        "tmdb_supported": False,
        "hls_referer": False,
    },
    "atlantic": {
        "base_url": "https://atlantic.st/",
        "movie_url": "https://atlantic.st/embed/movie/{}/",
        "meta_movie_url": "https://db.wecollege.net/3/movie/{}?append_to_response=external_ids&language=en",
        "tv_url": "https://atlantic.st/embed/tv/{}/{}/{}",
        "meta_tv_url": "https://db.wecollege.net/3/tv/{}?append_to_response=external_ids&language=en",
        "url_markers": [],
        "tmdb_supported": True,
    },
}

def is_tracking_request(url: str) -> bool:
    """Vérifie si l'URL appartient à un domaine publicitaire ou de tracking."""
    return any(domain in url for domain in ADS_AND_TRACKING_DOMAINS)


def _safe_component(value: str, fallback: str) -> str:
    """Convertit un titre externe en nom de dossier/fichier sûr."""
    component = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value).strip(" .")
    return component or fallback


def _detect_quality_hint(*sources: str) -> str | None:
    haystack = " ".join(s.lower() for s in sources if isinstance(s, str))
    if re.search(r"2160p|\b4k\b|\buhd\b", haystack):
        return "4K"
    for quality in ("1080p", "720p", "480p"):
        if quality in haystack:
            return quality
    return None

async def _is_hls_playlist_response(response) -> bool:
    """Détermine si une réponse réseau est une playlist HLS (.m3u8)."""
    path = response.url.lower().split("?", 1)[0]
    if path.endswith((".m3u8", ".m3u")) or "m3u8" in path.rsplit("/", 1)[-1]:
        return True

    headers = response.headers
    content_type = headers.get("content-type", "").lower()
    if "mpegurl" in content_type:
        return True

    # Dernier recours : sniffing du contenu pour les requêtes XHR/fetch dont
    # l'URL n'a pas d'extension (ex. `/s?data=...`) et le type est générique.
    if response.request.resource_type not in ("xhr", "fetch", "media", "other"):
        return False
    if content_type and not content_type.startswith(("text/plain", "application/octet-stream")):
        return False
    try:
        if int(headers.get("content-length", "0")) > 2_000_000:
            return False
        body = await response.body()
    except Exception:
        return False
    return body.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"#EXTM3U")


def _hls_embedded_headers(url: str) -> dict[str, str]:
    """Extrait les en-têtes (origin/referer) encodés dans le paramètre `q` des
    master.m3u8 d'Atlantic (JSON en base64url, clé `h`). Les CDN de segments
    exigent ces valeurs et renvoient 403 sinon."""
    try:
        q = parse_qs(urlsplit(url).query).get("q", [""])[0]
        if not q:
            return {}
        payload = json.loads(base64.urlsafe_b64decode(q + "=" * (-len(q) % 4)))
        headers = payload.get("h", {}) if isinstance(payload, dict) else {}
        return {
            k.lower(): v for k, v in headers.items()
            if isinstance(k, str) and isinstance(v, str) and v
        }
    except Exception:
        return {}


def hls_header_sets(url: str, provider: str) -> list[tuple[str | None, str | None]]:
    """Jeux (referer, origin) à essayer, par ordre de priorité.

    Les proxys `m3u8-proxy` et les segments `totallyacdn.cc` exigent le site du
    provider (atlantic.st) et renvoient 403 avec l'origine du `q`, alors que
    certains CDN directs (ex. neoncreek.top) exigent celle du `q` (movy.sx)."""
    provider_base_url = STREAMING_PROVIDERS[provider]["base_url"]
    sets: list[tuple[str | None, str | None]] = [
        (provider_base_url, provider_base_url.rstrip("/"))
    ]
    embedded = _hls_embedded_headers(url)
    referer = embedded.get("referer")
    origin = embedded.get("origin")
    if referer or origin:
        candidate = (
            referer or (origin or "") + "/",
            origin or (referer or "").rstrip("/"),
        )
        if candidate not in sets:
            sets.append(candidate)

    # (None, None) = aucun Referer/Origin. vsembed (fernandfeather.space) répond 403
    # dès qu'un Referer étranger est présent, alors qu'Atlantic exige le sien.
    if STREAMING_PROVIDERS[provider].get("hls_referer", True):
        sets.append((None, None))
    else:
        sets.insert(0, (None, None))
    return sets


_HLS_ATTR_RE = re.compile(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)')


def _parse_hls_attrs(line: str) -> dict[str, str]:
    return {k: v.strip('"') for k, v in _HLS_ATTR_RE.findall(line.split(":", 1)[1])}


def hls_master_variants(
    master_text: str, master_url: str, max_height: int = 1080
) -> list[dict]:
    """Liste les variantes d'un master HLS (height, bandwidth, codecs, url, audio_url),
    triées par ordre de préférence : 1080p d'abord, puis qualités inférieures
    décroissantes, puis éventuelles qualités supérieures, puis hauteur inconnue."""
    lines = [l.strip() for l in master_text.splitlines() if l.strip()]

    audio_groups: dict[str, str] = {}
    for line in lines:
        if line.startswith("#EXT-X-MEDIA:"):
            attrs = _parse_hls_attrs(line)
            if attrs.get("TYPE") == "AUDIO" and attrs.get("URI"):
                group = attrs.get("GROUP-ID", "")
                if group not in audio_groups or attrs.get("DEFAULT") == "YES":
                    audio_groups[group] = urljoin(master_url, attrs["URI"])

    variants: list[dict] = []
    seen: set[str] = set()
    for index, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF:"):
            continue
        uri = next((l for l in lines[index + 1:] if not l.startswith("#")), None)
        if not uri:
            continue
        url = urljoin(master_url, uri)
        if url in seen:
            continue
        seen.add(url)

        attrs = _parse_hls_attrs(line)
        label = re.search(r"(\d{3,4})p", f"{attrs.get('NAME', '')} {uri}".lower())
        height: int | None = int(label.group(1)) if label else None
        resolution = re.fullmatch(r"(\d+)x(\d+)", attrs.get("RESOLUTION", ""))
        if height is None and resolution:
            width, res_height = int(resolution.group(1)), int(resolution.group(2))
            # Les films en cinémascope (1920x804) restent du "1080p".
            height = max(res_height, round(width * 9 / 16))
        bandwidth_raw = attrs.get("BANDWIDTH", "")
        bandwidth = int(bandwidth_raw) if bandwidth_raw.isdigit() else 0
        variants.append({
            "height": height,
            "bandwidth": bandwidth,
            "codecs": attrs.get("CODECS", ""),
            "url": url,
            "audio_url": audio_groups.get(attrs.get("AUDIO", "")),
        })

    def preference(item: dict):
        height, bandwidth = item["height"], item["bandwidth"]
        if height is None:
            return (2, 0, -bandwidth)
        if height <= max_height:
            return (0, -height, -bandwidth)
        return (1, height, -bandwidth)

    variants.sort(key=preference)
    return variants


def _hls_variant_candidates(
    master_text: str, master_url: str, max_height: int = 1080
) -> list[tuple[int | None, str, str | None]]:
    """Variantes sous la forme (hauteur, url, url_audio), par ordre de préférence."""
    return [
        (v["height"], v["url"], v["audio_url"])
        for v in hls_master_variants(master_text, master_url, max_height)
    ]


async def _run_ffmpeg(
    target: str, audio_target: str | None, output_path: Path, request_headers: str
) -> None:
    # `-extension_picky 0` : les segments Atlantic n'ont pas d'extension
    # (`.../s?data=...`) et FFmpeg >= 5.1 les rejette sinon ("Invalid data found").
    input_options = [
        "-extension_picky", "0",
        "-user_agent", "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
        *(["-headers", request_headers] if request_headers else []),
    ]
    inputs = [*input_options, "-i", target]
    maps = ["-map", "0:v:0", "-map", "0:a?"]
    if audio_target:
        inputs += [*input_options, "-i", audio_target]
        maps = ["-map", "0:v:0", "-map", "1:a:0"]

    # Pas de `-bsf:a aac_adtstoasc` forcé : il fait échouer FFmpeg dès que l'audio
    # n'est pas de l'AAC ADTS (mp3, opus...). Le muxer MP4 l'ajoute tout seul si besoin.
    # `-map` évite d'embarquer des pistes (sous-titres WebVTT...) incompatibles avec MP4.
    ffmpeg_arguments = [
        "ffmpeg",
        "-y",
        *inputs,
        *maps,
        "-c", "copy",
        "-movflags", "+faststart",
        str(output_path)
    ]

    ffmpeg_process = await asyncio.create_subprocess_exec(
        *ffmpeg_arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Lecture en continu de la sortie standard d'erreur de FFmpeg pour afficher la progression
    assert ffmpeg_process.stderr is not None
    last_lines: list[str] = []
    while True:
        line = await ffmpeg_process.stderr.readline()
        if not line:
            break

        output_text = line.decode('utf-8', errors='replace').strip()
        if output_text:
            last_lines = (last_lines + [output_text])[-8:]
        if "frame=" in output_text or "Error" in output_text:
            print(f"\r[Téléchargement] {output_text}", end="", flush=True)

    await ffmpeg_process.wait()

    if ffmpeg_process.returncode != 0:
        details = " | ".join(last_lines)
        raise RuntimeError(
            f"Le téléchargement a échoué (Code de sortie FFmpeg : {ffmpeg_process.returncode}). {details}"
        )
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise RuntimeError(f"FFmpeg n'a pas créé de fichier vidéo valide : {output_path}")

    output_path = output_path.resolve()
    print(f"\n\n[✅ SUCCÈS] Fichier finalisé et optimisé : {output_path}")


async def _download_from_stream(
    target: str,
    output_path: Path,
    provider: str,
    master_content: str | None = None,
) -> str | None:
    """Télécharge le flux en visant la 1080p, puis en descendant en qualité si une
    variante échoue. Retourne la qualité téléchargée (ex. "1080p") si connue."""
    header_blobs = [
        (f"Referer: {ref}\r\n" if ref else "") + (f"Origin: {org}\r\n" if org else "")
        for ref, org in hls_header_sets(target, provider)
    ]

    candidates: list[tuple[int | None, str, str | None]] = []
    if master_content and "#EXT-X-STREAM-INF" in master_content:
        candidates = _hls_variant_candidates(master_content, target)
    # Dernier recours : laisser FFmpeg choisir lui-même dans le master.
    candidates.append((None, target, None))

    last_error: RuntimeError | None = None
    for height, url, audio_url in candidates:
        label = f"{height}p" if height else "qualité automatique"
        print(f"\n[INFO] Tentative de téléchargement ({label})...")
        for request_headers in header_blobs:
            try:
                await _run_ffmpeg(url, audio_url, output_path, request_headers)
                return f"{height}p" if height else None
            except RuntimeError as e:
                last_error = e
                output_path.unlink(missing_ok=True)
        print(f"\n[⚠️] Échec en {label}, essai de la qualité suivante...")

    assert last_error is not None
    raise last_error

async def extract_video_stream(
    provider: str,
    tmdb_id: str,
    content_type: str = "movie",
    season: int | None = None,
    episode: int | None = None,
    output_dir: str | Path | None = None,
    stream_only: bool = False,
) -> Path | str | dict:
    target_playlist_url = None
    master_content = ""

    content_type = "tv" if content_type in ("tv", "series", "show") else "movie"
    is_tv = content_type in ("tv", "series", "show")

    if provider not in STREAMING_PROVIDERS:
        raise ValueError(f"Provider non supporté : {provider}")

    cfg = load_config()
    tmdb_data = await tmdb_lookup(tmdb_id, content_type, cfg['tmdb_api_key'], cfg['tmdb_language'])

    if STREAMING_PROVIDERS[provider]['tmdb_supported']:
        db_id = tmdb_id
    else:
        db_id = tmdb_data['imdb_id']

    # 1. Validation et construction des URLs selon le type de contenu
    if is_tv:
        if season is None or episode is None:
            raise ValueError("Pour une série, la saison et l'épisode sont obligatoires.")
        video_player_url = STREAMING_PROVIDERS[provider]['tv_url'].format(db_id, season, episode)
        metadata_api_url = STREAMING_PROVIDERS[provider]['meta_tv_url'].format(db_id, season, episode)
        default_filename = f"{tmdb_data['title']} S{season:02d} E{episode:02d}.mp4"
    else:
        video_player_url = STREAMING_PROVIDERS[provider]['movie_url'].format(db_id)
        metadata_api_url = STREAMING_PROVIDERS[provider]['meta_movie_url'].format(db_id)
        default_filename = f"{tmdb_data['title']}.mp4"

    download_root = Path(output_dir) if output_dir is not None else Path(__file__).resolve().parent / "downloads"

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        browser_context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="fr-FR"
        )

        api_response = await browser_context.request.get(metadata_api_url)

        output_filename = default_filename
        media_title = db_id
        quality_hint = None
        if api_response.ok:
            try:
                metadata = await api_response.json()

                if provider == 'vsembed':
                    data = metadata.get("data", {}) if isinstance(metadata, dict) else {}
                else :
                  data = metadata

                if isinstance(data, dict):
                    for title_key in ("title", "name", "movie_name", "series_name", "show_name"):
                        candidate = data.get(title_key)
                        if isinstance(candidate, str) and candidate.strip():
                            media_title = candidate.strip()
                            break

                    if provider == 'vsembed':
                        file_path = data.get("file_name", "")

                        if isinstance(file_path, str) and file_path.strip():
                            # Ne jamais réutiliser les répertoires fournis par l'API.
                            output_filename = file_path.replace("\\", "/").rsplit("/", 1)[-1]
                            if Path(output_filename).suffix.lower() not in (".mp4", ".mkv"):
                                output_filename = f"{Path(output_filename).stem or db_id}.mp4"
                    else:
                        output_filename = default_filename

                    quality_hint = _detect_quality_hint(
                        output_filename,
                        str(data.get("quality", "")),
                        str(data.get("label", "")),
                    )
            except Exception as e:
                print(f"[ATTENTION] Impossible de parser les métadonnées JSON : {e}")

        dl_output_path = Path("/")
        if not stream_only:
            media_folder = _safe_component(media_title, db_id)
            if is_tv:
                destination_dir = download_root / "TV Shows" / media_folder / f"Season {season:02d}" / provider
            else:
                destination_dir = download_root / "Movies" / media_folder / provider
            destination_dir.mkdir(parents=True, exist_ok=True)
            dl_output_path = destination_dir / _safe_component(output_filename, default_filename)

        # 3. Contournement des protections anti-débogage
        async def bypass_anti_debugging(route):
            await route.fulfill(status=200, content_type="application/javascript", body="/* neutralised */")

        await browser_context.route(re.compile(r".*disable-devtool.*\.js.*"), bypass_anti_debugging)

        # 4. Interception des requêtes réseau pour capturer le flux vidéo
        async def intercept_network_response(response):
            nonlocal target_playlist_url, quality_hint, master_content
            if is_tracking_request(response.url):
                return

            if target_playlist_url or response.status != 200:
                return

            markers = STREAMING_PROVIDERS[provider]["url_markers"]
            if markers and not all(marker in response.url for marker in markers):
                return

            try:
                if not await _is_hls_playlist_response(response):
                    return

                # Lecture du corps uniquement pour affiner la qualité (optionnel).
                content = ""
                try:
                    content = await response.text()
                except Exception:
                    pass

                # Une autre réponse a pu être validée pendant l'await.
                if target_playlist_url:
                    return

                target_playlist_url = response.url
                master_content = content
                quality_hint = _detect_quality_hint(response.url, content) or quality_hint
                kind = "master" if "#EXT-X-STREAM-INF" in content else "media"
                print(f"\n[🔥 SUCCÈS] Playlist HLS {kind} interceptée : {response.url}")
            except Exception as e:
                print(f"[⚠️] Impossible d'analyser la réponse HLS : {e}")

        browser_context.on("response", intercept_network_response)

        page = await browser_context.new_page()

        # 5. Fermeture automatique des fenêtres contextuelles (pop-ups publicitaires)
        async def close_popup_tabs(new_page):
            print("\n[🛡️] Fenêtre publicitaire bloquée et fermée automatiquement.")
            try:
                await new_page.close()
            except Exception:
                pass

        browser_context.on("page", close_popup_tabs)

        print("Chargement du lecteur vidéo...")
        await page.goto(video_player_url, wait_until="domcontentloaded")

        # Initialisation du lecteur via un clic simulé
        await asyncio.sleep(2)
        try:
            # Clic au centre de la zone d'affichage (viewport 1280x720 -> centre 640x360)
            await page.mouse.click(640, 360)
        except Exception as e:
            print(f"[ATTENTION] Échec du clic d'initialisation : {e}")

        # 6. Attente active de la capture de l'URL du flux
        try:
            for _ in range(120):  # Limite d'attente fixée à 2 minutes max
                if target_playlist_url:
                    break
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n[INFO] Opération annulée par l'utilisateur.")
        finally:
            await browser_context.close()
            await browser.close()

    if not target_playlist_url:
        raise RuntimeError("Impossible de récupérer le lien du flux vidéo (M3U8).")

    if not stream_only:
        print(f"[INFO] Téléchargement du fichier : {dl_output_path}")
        downloaded_quality = await _download_from_stream(
            target_playlist_url,
            dl_output_path,
            provider=provider,
            master_content=master_content,
        )
        quality_hint = downloaded_quality or quality_hint

    return {
        "playlist_url": target_playlist_url,
        "provider": provider.upper(),
        "filename": _safe_component(output_filename, default_filename),
        "title": media_title,
        "quality": quality_hint,
        "dl_output_path": dl_output_path,
    }


# --- Exemples de lancement ---
if __name__ == "__main__":
    # Exemple 1 : Téléchargement d'un film (par défaut)
    print(asyncio.run(extract_video_stream(tmdb_id="1667", content_type="tv", stream_only=True, provider="atlantic", season=52, episode=4)))
