# Oxarus

Oxarus est un provider local pour [Nuvio](https://github.com/tapframe/Nuvio)
qui transforme une requête TMDB en flux vidéo lisible par Nuvio. Il récupère
l'ID IMDb correspondant, ouvre le lecteur source avec Playwright, détecte sa
playlist HLS puis la diffuse au client via un proxy local.

Le mode HLS est activé par défaut : la lecture commence dès que la playlist et
les premiers segments sont disponibles, sans attendre la création complète
d'un fichier vidéo par FFmpeg. Le téléchargement MP4/MKV reste disponible en
mode de compatibilité ou d'archivage.

> Utilisez ce projet uniquement avec des contenus et des sources pour lesquels
> vous disposez des droits nécessaires et dans le respect des conditions
> d'utilisation applicables.

## Fonctionnement

```text
Nuvio
  │
  │ GET /api/streams?tmdb_id=...
  ▼
Oxarus / FastAPI
  ├─ TMDB : TMDB ID → IMDb ID
  ├─ Playwright : ouverture du lecteur et capture de l'URL .m3u8
  ├─ Mode download_file=false
  │    └─ URL HLS locale → proxy playlist, segments et clés
  └─ Mode download_file=true
       └─ FFmpeg → fichier MP4/MKV → endpoint /media avec Range
```

Le proxy ne fournit pas de proxy HTTP ouvert : chaque ressource HLS est
associée à une session créée par Oxarus et doit rester sur le même hôte HTTPS
que la playlist capturée. Les sessions expirent après
`hls_session_ttl_seconds` secondes d'inactivité.

## Prérequis

- Python 3.10 ou plus récent ;
- FFmpeg uniquement si le mode MP4 est utilisé ;
- une clé API TMDB ;
- un navigateur Chromium compatible avec Playwright ;
- un client Nuvio capable de lire les flux HLS (`.m3u8`) pour le mode direct.

## Installation

```bash
git clone <URL_DU_DEPOT>
cd Oxarus

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
playwright install chromium
```

Installez ensuite FFmpeg selon votre système si vous voulez utiliser le mode
de repli :

```bash
# Debian/Ubuntu
sudo apt install ffmpeg
```

## Configuration

Modifiez `config.json` :

```json
{
  "host": "0.0.0.0",
  "port": 9500,
  "public_url": "http://192.168.1.10:9500",
  "api_key": "changez-cette-cle",
  "tmdb_api_key": "VOTRE_CLE_TMDB",
  "download_dir": "./downloads",
  "max_concurrent_downloads": 1,
  "download_file": false,
  "direct_hls": true,
  "hls_session_ttl_seconds": 7200,
  "retry_cooldown_seconds": 300
}
```

### Options principales

| Option | Description |
|---|---|
| `host` | Adresse d'écoute du serveur. `0.0.0.0` permet l'accès depuis le réseau local. |
| `port` | Port HTTP du serveur. |
| `public_url` | URL réellement accessible par Nuvio. Indispensable si le serveur est derrière un proxy ou possède plusieurs interfaces réseau. |
| `cors_origins` | Origines autorisées pour les appels depuis une autre application/appareil. `["*"]` convient à un serveur local ; préférez une liste d'origines connue si nécessaire. |
| `api_key` | Clé optionnelle exigée sur les endpoints API, média et HLS. |
| `tmdb_api_key` | Clé TMDB v3 ou jeton TMDB v4. |
| `download_dir` | Répertoire des fichiers téléchargés en mode MP4. |
| `max_concurrent_downloads` | Nombre maximal de captures/téléchargements simultanés. |
| `download_file` | `false` diffuse directement en HLS sans créer de fichier ; `true` attend FFmpeg et conserve un MP4/MKV. |
| `direct_hls` | `true` pour publier immédiatement le proxy HLS, `false` pour utiliser FFmpeg et la bibliothèque locale. |
| `hls_session_ttl_seconds` | Durée d'inactivité avant suppression d'une session HLS. |
| `retry_cooldown_seconds` | Délai avant de relancer automatiquement un job échoué. |
| `video_extensions` | Extensions acceptées par la bibliothèque locale. |

`public_url` doit être joignable depuis l'appareil qui exécute Nuvio. Une
adresse `localhost` ou `127.0.0.1` ne fonctionnera pas si Nuvio est installé
sur un autre appareil.

## Utilisation depuis un autre appareil du réseau local

Le serveur écoute par défaut sur toutes les interfaces (`0.0.0.0`). Pour que
la lecture fonctionne depuis un téléphone, une TV ou un autre ordinateur :

1. trouvez l'adresse LAN de la machine qui exécute Oxarus (`ip addr` sous
   Linux, `ipconfig` sous Windows) ;
2. définissez `public_url` avec cette adresse, par exemple :

   ```json
   {
     "public_url": "http://192.168.1.25:9500"
   }
   ```

3. autorisez le port TCP `9500` dans le pare-feu de cette machine ;
4. ajoutez `http://192.168.1.25:9500/manifest.json` dans Nuvio.

Les URLs retournées par `/api/streams`, y compris les URLs HLS et `/media`,
utiliseront alors directement l'adresse LAN. Les requêtes CORS sont activées
par défaut afin qu'une application Nuvio servie depuis une autre origine
puisse récupérer le manifest, les playlists et les segments. Pour restreindre
les origines :

```json
{
  "cors_origins": [
    "http://192.168.1.50:3000",
    "https://nuvio.example"
  ]
}
```

Le client et le serveur doivent être sur le même réseau routable. Le Wi-Fi
invité, l'isolation des clients Wi-Fi, un VPN ou un pare-feu peuvent empêcher
un appareil de joindre l'adresse LAN. Depuis l'autre appareil, vérifiez :

```bash
curl http://192.168.1.25:9500/health
```

## Lancement

```bash
source .venv/bin/activate
python server.py
```

Le serveur affiche l'URL du manifest, par exemple :

```text
http://192.168.1.10:9500/manifest.json
```

Ajoutez cette URL dans Nuvio. Le provider généré est disponible à
`/providers/localserver.js`.

Vérification rapide :

```bash
curl http://127.0.0.1:9500/health
curl http://127.0.0.1:9500/manifest.json
```

Avec une clé API :

```bash
curl -H 'x-api-key: changez-cette-cle' \
  http://127.0.0.1:9500/api/library
```

## Mode HLS direct

Le paramètre recommandé pour choisir le comportement est `download_file` :

```json
{
  "download_file": false
}
```

Avec `false`, aucun fichier vidéo n'est téléchargé : Oxarus capture la playlist
HLS et la diffuse immédiatement via son proxy local. Pour activer la création
d'un fichier :

```json
{
  "download_file": true
}
```

`download_file` est prioritaire sur `direct_hls`. Le paramètre historique
`direct_hls` est conservé pour compatibilité de configuration, mais il est
recommandé d'utiliser `download_file` pour les nouvelles installations.

Avec `"download_file": false`, une requête de stream déclenche uniquement la
capture de la playlist source. Dès que l'URL `.m3u8` est détectée, l'API
retourne un stream local :

```json
{
  "status": "ready",
  "streams": [
    {
      "format": "hls",
      "url": "http://192.168.1.10:9500/hls/<session>/<ressource>"
    }
  ]
}
```

Oxarus réécrit les URI de la playlist avant de la transmettre au lecteur :

- segments vidéo et audio ;
- playlists enfants d'une master playlist ;
- clés de chiffrement (`URI="..."`) ;
- URI relatives et absolues.

L'URL du fournisseur est encapsulée dans l'URL du proxy et n'est pas utilisée
directement comme destination par Nuvio ; son contenu encodé reste toutefois
réversible. Utilisez `api_key` et HTTPS si les URL ou jetons source doivent
être protégés contre l'observation du trafic. Une URL HLS peut expirer côté
fournisseur ; dans ce cas, relancez la requête de stream afin de capturer une
nouvelle session.

## Mode MP4/MKV avec FFmpeg

Certains clients ne savent pas lire HLS. Pour conserver le comportement
historique, activez le téléchargement :

```json
{
  "download_file": true
}
```

Oxarus attend alors la fin de FFmpeg, indexe le fichier dans `library.json` et
retourne une URL `/media/...`. Cet endpoint prend en charge les requêtes HTTP
`Range`, ce qui permet le seek dans les lecteurs compatibles.

Le téléchargeur peut également être utilisé directement :

```bash
python downloader.py
```

En interne, `extract_and_download_video(..., stream_only=True)` retourne
l'URL HLS capturée ; avec `stream_only=False`, il conserve le téléchargement
FFmpeg.

## API HTTP

Toutes les routes protégées acceptent la clé dans l'en-tête
`x-api-key` ou dans le paramètre `key`.

| Méthode | Route | Description |
|---|---|---|
| `GET` | `/` | Informations et liens principaux. |
| `GET` | `/health` | Vérifie que le serveur répond. |
| `GET` | `/manifest.json` | Manifest Nuvio. |
| `GET` | `/providers/localserver.js` | Provider JavaScript généré. |
| `GET` | `/api/streams` | Recherche un film ou épisode et retourne son stream. |
| `GET` | `/api/library` | Liste les fichiers MP4/MKV indexés. |
| `GET` | `/api/jobs` | Liste les jobs en cours ou terminés. |
| `GET` | `/hls/{session}/{ressource}` | Proxy HLS interne. |
| `GET`, `HEAD` | `/media/{id}/{filename}` | Fichier local avec support Range. |

### `/api/streams`

Film par TMDB :

```bash
curl -G 'http://127.0.0.1:9500/api/streams' \
  --data-urlencode 'tmdb_id=550' \
  --data-urlencode 'type=movie' \
  --data-urlencode 'wait=90'
```

Épisode de série :

```bash
curl -G 'http://127.0.0.1:9500/api/streams' \
  --data-urlencode 'tmdb_id=1399' \
  --data-urlencode 'type=tv' \
  --data-urlencode 'season=1' \
  --data-urlencode 'episode=1' \
  --data-urlencode 'wait=90'
```

Paramètres acceptés :

- `tmdb_id` : identifiant TMDB, ou directement un identifiant IMDb commençant
  par `tt` ;
- `imdb_id` : alternative à `tmdb_id` ;
- `type` : `movie` ou `tv` ;
- `season` et `episode` : obligatoires pour une série ;
- `wait` : nombre de secondes pendant lesquelles la requête attend un job,
  limité à 120 secondes.

Les états possibles sont `queued`, `downloading`, `ready` et `failed`.

## Dépannage

### Le manifest est inaccessible dans Nuvio

Vérifiez `public_url`, l'adresse IP du serveur, le port ouvert dans le
pare-feu et l'accessibilité depuis l'appareil Nuvio :

```bash
curl http://ADRESSE_DU_SERVEUR:9500/health
```

### La lecture reste vide en mode HLS

Vérifiez que le client accepte les URL `.m3u8`, que l'URL source est toujours
valide et que le serveur peut joindre la source HTTPS. Consultez les logs
d'`python server.py` et relancez le stream pour obtenir une nouvelle capture.

### Le job échoue pendant la capture

La page source peut nécessiter un délai plus long, un lecteur différent ou
être indisponible. Vérifiez également l'installation de Chromium :

```bash
playwright install chromium
```

### Le mode MP4 échoue

Vérifiez que FFmpeg est installé et disponible dans le `PATH` :

```bash
ffmpeg -version
```

Vérifiez aussi les droits d'écriture dans `download_dir`.

### Authentification

Si `api_key` est définie, ajoutez-la à Nuvio via la configuration générée ou
utilisez l'en-tête `x-api-key`. Évitez de transmettre une clé dans des logs ou
de la partager publiquement.

## Développement et validation

Compilation Python :

```bash
python -m py_compile server.py downloader.py
```

Lint :

```bash
ruff check server.py downloader.py
```

Le projet ne contient pas de suite de tests automatisés. Les tests de lecture
réelle dépendent de la disponibilité de la source distante, de TMDB et de
Chromium ; ils doivent donc être effectués dans un environnement autorisé et
connecté.

## Structure du projet

```text
.
├── config.json                    Configuration du serveur et du provider
├── downloader.py                  Capture Playwright et téléchargement FFmpeg
├── server.py                      API FastAPI, jobs et proxies HLS/HTTP
├── provider/
│   └── localserver.template.js    Provider JavaScript généré pour Nuvio
├── requirements.txt               Dépendances Python
├── downloads/                     Fichiers téléchargés (créé à l'exécution)
└── library.json                   Index local des fichiers MP4/MKV
```

`downloads/` et les fichiers vidéo sont exclus du dépôt par `.gitignore`.
N'ajoutez jamais de clé TMDB ou de clé API réelle à Git.
