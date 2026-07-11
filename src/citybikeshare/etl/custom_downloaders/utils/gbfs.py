"""Shared GBFS helpers. A GBFS system exposes a discovery document (`gbfs.json`) that lists
named feeds; `station_information` is the list we harvest into the committed station coordinates.
Kept thin: resolve the feed URL and save the bytes — the merge is the coordinates step's job."""

import json

import requests

from citybikeshare.context import PipelineContext


def _resolve_feed_url(gbfs_url: str, feed_name: str) -> str:
    """URL of a named feed from a GBFS discovery doc. `data` is either ``{feeds: [...]}`` or
    language-keyed (``{en: {feeds: [...]}}``); take the first language when keyed."""
    doc = requests.get(gbfs_url, timeout=(30, 120)).json()
    data = doc["data"]
    feeds = data.get("feeds") or next(iter(data.values()))["feeds"]
    for feed in feeds:
        if feed["name"] == feed_name:
            return feed["url"]
    raise ValueError(f"GBFS discovery {gbfs_url} has no '{feed_name}' feed")


def download_station_information(context: PipelineContext, gbfs_url: str) -> None:
    """Fetch the live station list into ``metadata/station_information.json``.

    ``gbfs_url`` is the system's ``gbfs.json`` discovery document. The pre-transform
    ``refresh_station_coordinates`` step merges the saved snapshot into the committed,
    cumulative coordinates (never-drop), so stations survive after they leave this live feed.
    """
    info_url = _resolve_feed_url(gbfs_url, "station_information")
    dest = context.metadata_directory / "station_information.json"
    dest.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(info_url, timeout=(30, 120))
    response.raise_for_status()
    dest.write_text(
        json.dumps(response.json(), ensure_ascii=False, indent=4), encoding="utf-8"
    )
    n = len(response.json()["data"]["stations"])
    print(f"Saved {n} GBFS stations → {dest}")
