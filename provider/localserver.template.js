/**
 * Local Server provider for Nuvio
 *
 * Fichier GÉNÉRÉ dynamiquement par server.py à partir de config.json
 * (les valeurs __XXX__ sont injectées à chaque requête).
 *
 * Écrit en chaînes de Promises (pas d'async/await) car Hermes ne supporte
 * pas async/await dans le code de plugin chargé dynamiquement.
 */

var BASE_URL = __BASE_URL__;
var API_KEY = __API_KEY__;
var MAX_WAIT_MS = __MAX_WAIT_MS__;
var POLL_SECONDS = __POLL_SECONDS__;
var TAG = "[LocalServer]";

function buildUrl(tmdbId, mediaType, season, episode, waitSeconds) {
    var url = BASE_URL + "/api/streams" +
        "?tmdb_id=" + encodeURIComponent(String(tmdbId)) +
        "&type=" + (mediaType === "tv" ? "tv" : "movie") +
        "&wait=" + waitSeconds;

    if (mediaType === "tv") {
        url += "&season=" + encodeURIComponent(String(season)) +
               "&episode=" + encodeURIComponent(String(episode));
    }
    if (API_KEY) {
        url += "&key=" + encodeURIComponent(API_KEY);
    }
    return url;
}

function fetchJson(url) {
    return fetch(url, { headers: { "Accept": "application/json" } })
        .then(function (res) {
            if (!res.ok) {
                return res.text().then(function (body) {
                    throw new Error("HTTP " + res.status + " " + String(body).slice(0, 200));
                });
            }
            return res.json();
        });
}

/**
 * Interroge le serveur en boucle jusqu'à ce que le flux HLS/fichier soit prêt,
 * que le téléchargement échoue, ou que le délai maximum expire.
 */
function poll(tmdbId, mediaType, season, episode, startedAt) {
    var elapsed = Date.now() - startedAt;
    var remainingSec = Math.ceil((MAX_WAIT_MS - elapsed) / 1000);
    var waitSec = Math.max(1, Math.min(POLL_SECONDS, remainingSec));

    return fetchJson(buildUrl(tmdbId, mediaType, season, episode, waitSec))
        .then(function (data) {
            if (data.status === "ready") {
                console.log(TAG + " Prêt: " + (data.title || tmdbId));
                return data.streams || [];
            }
            if (data.status === "failed") {
                console.error(TAG + " Échec du téléchargement: " + (data.error || "inconnu"));
                return [];
            }
            if (Date.now() - startedAt >= MAX_WAIT_MS) {
                console.log(TAG + " Téléchargement toujours en cours (" + data.status +
                    "). Réessayez dans quelques instants.");
                return [];
            }
            console.log(TAG + " Statut: " + data.status + " … attente");
            return poll(tmdbId, mediaType, season, episode, startedAt);
        });
}

/**
 * Point d'entrée appelé par Nuvio.
 * @param {string} tmdbId
 * @param {string} mediaType - "movie" | "tv"
 * @param {number} season
 * @param {number} episode
 */
function getStreams(tmdbId, mediaType, season, episode) {
    console.log(TAG + " Request: " + mediaType + " " + tmdbId +
        (mediaType === "tv" ? " S" + season + "E" + episode : ""));

    return poll(tmdbId, mediaType, season, episode, Date.now())
        .catch(function (error) {
            console.error(TAG + " Error: " + error.message);
            return [];
        });
}

module.exports = { getStreams: getStreams };
