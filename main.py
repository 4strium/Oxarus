import asyncio
import re

from playwright.async_api import async_playwright

# Mots-clés pour filtrer les requêtes inutiles
JUNK_KEYWORDS = [
    "aliexpress", "histats", "adexchangerapid", "pubtrk", "alicdn",
    "google", "aidcgroup", "alilog", "gstatic", "googleapis", "click"
]

def is_junk(url: str) -> bool:
    return any(kw in url for kw in JUNK_KEYWORDS)

async def analyze_and_download(url: str, output_filename="video_finale.mp4"):
    master_m3u8_url = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="fr-FR"
        )

        # 1. Neutralise la lib anti-devtools
        async def kill_disable_devtool(route):
            await route.fulfill(status=200, content_type="application/javascript", body="/* disable-devtool neutralised */")
        await context.route(re.compile(r".*disable-devtool.*\.js.*"), kill_disable_devtool)

        # 2. Interception réseau pour capturer la playlist
        async def on_response(resp):
            nonlocal master_m3u8_url
            if is_junk(resp.url):
                return
                
            # On cherche spécifiquement la playlist contenant le jeton d'accès
            if ".m3u8" in resp.url and "token=" in resp.url and not master_m3u8_url:
                print("\n[🔥 BINGO] Playlist Master interceptée !")
                master_m3u8_url = resp.url

        context.on("response", on_response)

        page = await context.new_page()
        print("Chargement de la page... (Appuyez sur 'Play' dans le navigateur si la vidéo ne se lance pas seule)")
        
        await page.goto(url, wait_until="domcontentloaded")
        
        # 3. Boucle d'attente : on scrute jusqu'à ce que l'URL soit capturée
        try:
            for _ in range(120):  # Timeout de sécurité (2 minutes max)
                if master_m3u8_url:
                    break
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\nAnnulation par l'utilisateur...")
        finally:
            await context.close()
            await browser.close()

    # 4. Lancement de FFmpeg si on a bien récupéré l'URL
    if not master_m3u8_url:
        print("[ERREUR] Aucune URL m3u8 avec jeton n'a été interceptée.")
        return

    print("\n[🚀 Lancement du téléchargement FFmpeg]")
    print(f"URL source : {master_m3u8_url[:100]}...\n")

    # Commandes FFmpeg optimisées pour le streaming (Stremio / Nuvio)
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",                     # Écrase le fichier cible s'il existe déjà
        "-i", master_m3u8_url,    # URL source
        "-c", "copy",             # Copie les flux vidéo/audio sans ré-encoder (rapide et sans perte)
        "-bsf:a", "aac_adtstoasc",# Corrige les erreurs d'en-tête audio AAC fréquentes en HLS
        "-movflags", "+faststart",# Place l'index au début (crucial pour le streaming distant)
        output_filename           # Fichier de sortie
    ]

    # On lance FFmpeg en mode asynchrone pour afficher sa progression dans la console
    process = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # FFmpeg écrit ses logs de progression sur stderr
    while True:
        line = await process.stderr.readline()
        if not line:
            break
        
        # Optionnel: on filtre un peu la sortie pour ne garder que la progression
        texte = line.decode('utf-8').strip()
        if "frame=" in texte or "Error" in texte:
            # On utilise \r pour écraser la ligne et faire une barre de progression propre
            print(f"\r[FFmpeg] {texte}", end="", flush=True)

    await process.wait()
    
    if process.returncode == 0:
        print(f"\n\n[✅ SUCCÈS] Vidéo téléchargée avec succès et optimisée pour le streaming : {output_filename}")
    else:
        print(f"\n\n[❌ ERREUR] FFmpeg a terminé avec le code {process.returncode}. Vérifiez les logs.")

# Lancement
asyncio.run(analyze_and_download("https://vsembed.ru/embed/movie/tt27543632/", "femme_menage.mp4"))