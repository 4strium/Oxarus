
import httpx
from fastapi import HTTPException

_tmdb_cache: dict[str, dict] = {}

async def tmdb_lookup(tmdb_id: str, ctype: str, tmdb_api_key: str, tmdb_language: str) -> dict:
    """Retourne {"imdb_id", "title"} pour un ID TMDB (film ou série)."""
    cache_key = f"{ctype}:{tmdb_id}"
    if cache_key in _tmdb_cache:
        return _tmdb_cache[cache_key]

    if not tmdb_api_key or tmdb_api_key.startswith("VOTRE"):
        raise HTTPException(500, "tmdb_api_key manquante dans config.json")

    headers: dict[str, str] = {}
    params = {"append_to_response": "external_ids", "language": tmdb_language}
    if len(tmdb_api_key) > 40:  # jeton v4 (Bearer)
        headers["Authorization"] = f"Bearer {tmdb_api_key}"
    else:  # clé v3
        params["api_key"] = tmdb_api_key

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