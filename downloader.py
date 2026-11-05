import asyncio
import re
from pathlib import Path

from playwright.async_api import async_playwright

# Domaines liés à la publicité et au pistage à bloquer
ADS_AND_TRACKING_DOMAINS = [
    "aliexpress", "histats", "adexchangerapid", "pubtrk", "alicdn",
    "google", "aidcgroup", "alilog", "gstatic", "googleapis", "click"
]

def is_tracking_request(url: str) -> bool:
    """Vérifie si l'URL appartient à un domaine publicitaire ou de tracking."""
    return any(domain in url for domain in ADS_AND_TRACKING_DOMAINS)


def _safe_component(value: str, fallback: str) -> str:
    """Convertit un titre externe en nom de dossier/fichier sûr."""
    component = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value).strip(" .")
    return component or fallback


async def extract_and_download_video(
    imdb_id: str,
    content_type: str = "movie",
    season: int | None = None,
    episode: int | None = None,
    output_dir: str | Path | None = None,
    stream_only: bool = False,
) -> Path | str:
    target_playlist_url = None
    is_tv = content_type in ("tv", "series", "show")

    # 1. Validation et construction des URLs selon le type de contenu
    if is_tv:
        if season is None or episode is None:
            raise ValueError("Pour une série, la saison et l'épisode sont obligatoires.")
        video_player_url = f"https://vsembed.ru/embed/tv/{imdb_id}?season={season}&episode={episode}"
        metadata_api_url = f"https://data.vidsrc.sh/api.php?type=tv&imdb={imdb_id}&season={season}&episode={episode}"
        default_filename = f"{imdb_id}_S{season:02d}E{episode:02d}.mp4"
    else:
        video_player_url = f"https://vsembed.ru/embed/movie/{imdb_id}/"
        metadata_api_url = f"https://data.vidsrc.sh/api.php?type=movie&imdb={imdb_id}"
        default_filename = f"{imdb_id}.mp4"

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
        
        # 2. Récupération des métadonnées pour nommer le fichier dynamiquement
        print(f"Récupération des métadonnées depuis {metadata_api_url}...")
        api_response = await browser_context.request.get(metadata_api_url)
        
        output_filename = default_filename
        media_title = imdb_id
        if api_response.ok:
            try:
                metadata = await api_response.json()
                data = metadata.get("data", {}) if isinstance(metadata, dict) else {}
                if isinstance(data, dict):
                    for title_key in ("title", "name", "movie_name", "series_name", "show_name"):
                        candidate = data.get(title_key)
                        if isinstance(candidate, str) and candidate.strip():
                            media_title = candidate.strip()
                            break

                    file_path = data.get("file_name", "")
                    if isinstance(file_path, str) and file_path.strip():
                        # Ne jamais réutiliser les répertoires fournis par l'API.
                        output_filename = file_path.replace("\\", "/").rsplit("/", 1)[-1]
                        if Path(output_filename).suffix.lower() not in (".mp4", ".mkv"):
                            output_filename = f"{Path(output_filename).stem or imdb_id}.mp4"
            except Exception as e:
                print(f"[ATTENTION] Impossible de parser les métadonnées JSON : {e}")

        media_folder = _safe_component(media_title, imdb_id)
        if is_tv:
            destination_dir = download_root / "TV Shows" / media_folder / f"Season {season:02d}"
        else:
            destination_dir = download_root / "Movies" / media_folder
        destination_dir.mkdir(parents=True, exist_ok=True)
        output_path = destination_dir / _safe_component(output_filename, default_filename)

        print(f"Le fichier vidéo sera sauvegardé sous : {output_path}\n")

        # 3. Contournement des protections anti-débogage
        async def bypass_anti_debugging(route):
            await route.fulfill(status=200, content_type="application/javascript", body="/* neutralised */")
        
        await browser_context.route(re.compile(r".*disable-devtool.*\.js.*"), bypass_anti_debugging)

        # 4. Interception des requêtes réseau pour capturer le flux vidéo
        async def intercept_network_response(response):
            nonlocal target_playlist_url
            if is_tracking_request(response.url):
                return
                
            # Identification de la playlist principale (M3U8 avec jeton)
            if ".m3u8" in response.url and "token=" in response.url and not target_playlist_url:
                print("\n[🔥 SUCCÈS] Flux vidéo principal intercepté !")
                target_playlist_url = response.url

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

    # 7. Téléchargement et traitement via FFmpeg
    if not target_playlist_url:
        raise RuntimeError("Impossible de récupérer le lien du flux vidéo (M3U8).")

    if stream_only:
        print("\n[⚡ STREAMING] Flux HLS disponible immédiatement.")
        return target_playlist_url

    print("\n[🚀 Lancement du téléchargement]")
    print(f"Flux source : {target_playlist_url[:100]}...\n")

    ffmpeg_arguments = [
        "ffmpeg",
        "-y",
        "-i", target_playlist_url,
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
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
    while True:
        line = await ffmpeg_process.stderr.readline()
        if not line:
            break
        
        output_text = line.decode('utf-8').strip()
        if "frame=" in output_text or "Error" in output_text:
            print(f"\r[Téléchargement] {output_text}", end="", flush=True)

    await ffmpeg_process.wait()
    
    if ffmpeg_process.returncode != 0:
        raise RuntimeError(
            f"Le téléchargement a échoué (Code de sortie FFmpeg : {ffmpeg_process.returncode})."
        )
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise RuntimeError(f"FFmpeg n'a pas créé de fichier vidéo valide : {output_path}")

    output_path = output_path.resolve()
    print(f"\n\n[✅ SUCCÈS] Fichier finalisé et optimisé : {output_path}")
    return output_path

# --- Exemples de lancement ---
if __name__ == "__main__":
    # Exemple 1 : Téléchargement d'un film (par défaut)
    asyncio.run(extract_and_download_video(imdb_id="tt27543632", content_type="movie"))
    
    # Exemple 2 : Téléchargement d'un épisode de série
    asyncio.run(extract_and_download_video(
        imdb_id="tt11280740",
        content_type="tv", 
        season=1, 
        episode=2
    ))