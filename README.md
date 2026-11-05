# Oxarus

Oxarus is a local provider for [Nuvio](https://github.com/tapframe/Nuvio).
It lets Nuvio find and play movies and TV episodes through a server running
on your computer or home network.

Oxarus uses direct HLS streaming by default, so playback can start quickly
without first saving a complete video file. An optional download mode is
available for players that do not support HLS.

> Only use Oxarus with content and sources you are allowed to access. Follow
> all applicable laws and terms of service.

## What you need

- Python 3.10 or newer
- A [TMDB API key](https://www.themoviedb.org/settings/api)
- Chromium installed through Playwright
- FFmpeg only if you use download mode
- Nuvio, on the same machine or on a reachable device

## Install

```bash
git clone <repository-url>
cd Oxarus

python3 -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\activate         # Windows

python -m pip install -r requirements.txt
playwright install chromium
```

If you want download mode, install FFmpeg as well. On Debian or Ubuntu:

```bash
sudo apt install ffmpeg
```

## Configure

Open `config.json` and set your TMDB key:

```json
{
  "tmdb_api_key": "your-tmdb-key",
  "public_url": "",
  "download_file_before": false
}
```

The default settings are suitable for most local installations:

- `download_file_before: false` uses fast HLS streaming.
- Set it to `true` to download an MP4/MKV file before playback.
- Set `public_url` to the address Nuvio can reach, for example
  `http://192.168.1.25:9500`, when Nuvio runs on another device.
- `api_key` can optionally protect the server. If you set it, keep the value
  private and add it to the Nuvio provider configuration.

## Start Oxarus

```bash
source .venv/bin/activate
python server.py
```

The terminal displays the manifest address:

```text
http://your-computer:9500/manifest.json
```

Add this address to Nuvio. Then search for a movie or TV episode as usual.

When using another device, make sure both devices are on the same reachable
network and allow TCP port `9500` through the computer's firewall. Replace
`your-computer` with the computer's local IP address; `localhost` will not
work from another device.

You can check that the server is running at:

```text
http://your-computer:9500/health
```

## Choosing a playback mode

**HLS streaming (default)** is recommended for modern players. It does not
keep a complete video file and usually starts sooner.

**Download mode** is useful when your player cannot handle HLS. Oxarus saves
the video in `downloads/` and serves it to Nuvio. This mode needs FFmpeg and
may take longer before playback starts.

## Troubleshooting

**Nuvio cannot open the manifest**

Check `public_url`, the computer's local IP address, the firewall, and that
the device running Nuvio can open `/health` in a browser.

**Playback does not start**

Make sure Chromium was installed with `playwright install chromium`. For HLS,
confirm that the player supports `.m3u8` streams. Try the request again if the
source has expired.

**Download mode fails**

Check that FFmpeg is installed and available:

```bash
ffmpeg -version
```

Also check that Oxarus can write to the `downloads/` directory.

## Project files

```text
config.json                    Server settings
server.py                      Local server and Nuvio provider
downloader.py                  Browser-based stream capture
provider/localserver.template.js  Generated Nuvio provider template
downloads/                     Downloaded videos (created when needed)
library.json                   Local index of downloaded videos
```

Do not commit real TMDB or API keys to Git.
