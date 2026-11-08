import asyncio
import base64
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

from playwright.async_api import async_playwright

from configuration import load_config
from metadata import tmdb_lookup

# Advertising and tracking domains to block
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
    """Return whether the URL belongs to an advertising or tracking domain."""
    return any(domain in url for domain in ADS_AND_TRACKING_DOMAINS)


def _safe_component(value: str, fallback: str) -> str:
    """Convert an external title into a safe folder/file name."""
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
    """Return whether a network response is an HLS playlist (.m3u8)."""
    path = response.url.lower().split("?", 1)[0]
    if path.endswith((".m3u8", ".m3u")) or "m3u8" in path.rsplit("/", 1)[-1]:
        return True

    headers = response.headers
    content_type = headers.get("content-type", "").lower()
    if "mpegurl" in content_type:
        return True

    # Last resort: inspect the content for XHR/fetch requests whose URL has no
    # extension (e.g. `/s?data=...`) and whose type is generic.
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
    """Extract the headers (origin/referer) encoded in Atlantic's `q` parameter
    (base64url JSON, `h` key). Segment CDNs require these values and return 403 otherwise."""
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
    """Header sets (referer, origin) to try, in priority order.

    The `m3u8-proxy` and `totallyacdn.cc` segments require the provider site
    (atlantic.st) and return 403 with the `q` origin, while some direct CDNs
    (e.g. neoncreek.top) require the `q` origin (movy.sx)."""
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

    # (None, None) = no Referer/Origin. vsembed (fernandfeather.space) returns 403
    # when a foreign Referer is present, while Atlantic requires its own.
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
    """List master HLS variants (height, bandwidth, codecs, url, audio_url),
    sorted by preference: 1080p first, then descending lower qualities,
    then any higher qualities, then unknown height."""
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
            # Widescreen movies (1920x804) remain "1080p".
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
    """Return variants as (height, url, audio_url), in priority order."""
    return [
        (v["height"], v["url"], v["audio_url"])
        for v in hls_master_variants(master_text, master_url, max_height)
    ]


async def _run_ffmpeg(
    target: str, audio_target: str | None, output_path: Path, request_headers: str
) -> None:
    # `-extension_picky 0`: Atlantic segments have no extension
    # (`.../s?data=...`) and FFmpeg >= 5.1 rejects them otherwise ("Invalid data found").
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

    # Do not force `-bsf:a aac_adtstoasc`: it makes FFmpeg fail when audio is
    # not AAC ADTS (mp3, opus...). The MP4 muxer adds it automatically if needed.
    # `-map` avoids including tracks (WebVTT subtitles...) incompatible with MP4.
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

    # Continuously read FFmpeg's standard error output to display progress
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
            print(f"\r[Download] {output_text}", end="", flush=True)

    await ffmpeg_process.wait()

    if ffmpeg_process.returncode != 0:
        details = " | ".join(last_lines)
        raise RuntimeError(
            f"Download failed (FFmpeg exit code: {ffmpeg_process.returncode}). {details}"
        )
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise RuntimeError(f"FFmpeg did not create a valid video file: {output_path}")

    output_path = output_path.resolve()
    print(f"\n\n[✅ SUCCESS] File finalized and optimized: {output_path}")


async def _download_from_stream(
    target: str,
    output_path: Path,
    provider: str,
    master_content: str | None = None,
) -> str | None:
    """Download the stream targeting 1080p, then fall back if a variant fails.
    Return the downloaded quality (e.g. "1080p") when known."""
    header_blobs = [
        (f"Referer: {ref}\r\n" if ref else "") + (f"Origin: {org}\r\n" if org else "")
        for ref, org in hls_header_sets(target, provider)
    ]

    candidates: list[tuple[int | None, str, str | None]] = []
    if master_content and "#EXT-X-STREAM-INF" in master_content:
        candidates = _hls_variant_candidates(master_content, target)
    # Last resort: let FFmpeg choose from the master playlist.
    candidates.append((None, target, None))

    last_error: RuntimeError | None = None
    for height, url, audio_url in candidates:
        label = f"{height}p" if height else "automatic quality"
        print(f"\n[INFO] Download attempt ({label})...")
        for request_headers in header_blobs:
            try:
                await _run_ffmpeg(url, audio_url, output_path, request_headers)
                return f"{height}p" if height else None
            except RuntimeError as e:
                last_error = e
                output_path.unlink(missing_ok=True)
        print(f"\n[⚠️] Failed at {label}, trying the next quality...")

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
        raise ValueError(f"Unsupported provider: {provider}")

    cfg = load_config()
    tmdb_data = await tmdb_lookup(tmdb_id, content_type, cfg['tmdb_api_key'], cfg['tmdb_language'])

    if STREAMING_PROVIDERS[provider]['tmdb_supported']:
        db_id = tmdb_id
    else:
        db_id = tmdb_data['imdb_id']

    # 1. Validate and build URLs based on the content type
    if is_tv:
        if season is None or episode is None:
            raise ValueError("Season and episode are required for a series.")
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
                            # Never reuse directories supplied by the API.
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
                print(f"[WARNING] Unable to parse JSON metadata: {e}")

        dl_output_path = Path("/")
        if not stream_only:
            media_folder = _safe_component(media_title, db_id)
            if is_tv:
                destination_dir = download_root / "TV Shows" / media_folder / f"Season {season:02d}" / provider
            else:
                destination_dir = download_root / "Movies" / media_folder / provider
            destination_dir.mkdir(parents=True, exist_ok=True)
            dl_output_path = destination_dir / _safe_component(output_filename, default_filename)

        # 3. Bypass anti-debugging protections
        async def bypass_anti_debugging(route):
            await route.fulfill(status=200, content_type="application/javascript", body="/* neutralised */")

        await browser_context.route(re.compile(r".*disable-devtool.*\.js.*"), bypass_anti_debugging)

        # 4. Intercept network requests to capture the video stream
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

                # Read the body only to refine quality (optional).
                content = ""
                try:
                    content = await response.text()
                except Exception:
                    pass

                # Another response may have been validated while awaiting.
                if target_playlist_url:
                    return

                target_playlist_url = response.url
                master_content = content
                quality_hint = _detect_quality_hint(response.url, content) or quality_hint
                kind = "master" if "#EXT-X-STREAM-INF" in content else "media"
                print(f"\n[🔥 SUCCESS] HLS {kind} playlist intercepted: {response.url}")
            except Exception as e:
                print(f"[⚠️] Unable to analyze HLS response: {e}")

        browser_context.on("response", intercept_network_response)

        page = await browser_context.new_page()

        # 5. Automatically close popup windows (advertising pop-ups)
        async def close_popup_tabs(new_page):
            print("\n[🛡️] Advertising popup blocked and closed automatically.")
            try:
                await new_page.close()
            except Exception:
                pass

        browser_context.on("page", close_popup_tabs)

        print("Loading video player...")
        await page.goto(video_player_url, wait_until="domcontentloaded")

        # Initialize the player with a simulated click
        await asyncio.sleep(2)
        try:
            # Click the center of the display area (1280x720 viewport -> center 640x360)
            await page.mouse.click(640, 360)
        except Exception as e:
            print(f"[WARNING] Initialization click failed: {e}")

        # 6. Actively wait for the stream URL to be captured
        try:
            for _ in range(120):  # Maximum wait time is 2 minutes
                if target_playlist_url:
                    break
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n[INFO] Operation cancelled by user.")
        finally:
            await browser_context.close()
            await browser.close()

    if not target_playlist_url:
        raise RuntimeError("Unable to retrieve the video stream link (M3U8).")

    if not stream_only:
        print(f"[INFO] Downloading file: {dl_output_path}")
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


# --- Usage examples ---
if __name__ == "__main__":
    # Example 1: download a movie (default)
    print(asyncio.run(extract_video_stream(tmdb_id="1667", content_type="tv", stream_only=True, provider="atlantic", season=52, episode=4)))
