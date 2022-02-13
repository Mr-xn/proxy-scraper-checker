#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import logging
import re
from pathlib import Path
from random import shuffle
from shutil import rmtree
from time import perf_counter
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

from aiohttp import ClientSession
from aiohttp_socks import ProxyConnector

import config


class Proxy:
    def __init__(self, socket_address: str, ip: str) -> None:
        """
        Args:
            socket_address: ip:port
        """
        self.socket_address = socket_address
        self.ip = ip
        self.is_anonymous: Optional[bool] = None
        self.geolocation: str = "::None::None::None"
        self.timeout = float("inf")

    def update(self, info: Dict[str, str]) -> None:
        """Set geolocation and is_anonymous.

        Args:
            info: Response from http://ip-api.com/json.
        """
        country = info.get("country") or None
        region = info.get("regionName") or None
        city = info.get("city") or None
        self.geolocation = f"::{country}::{region}::{city}"
        self.is_anonymous = self.ip != info.get("query")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Proxy):
            return NotImplemented
        return self.socket_address == other.socket_address

    def __hash__(self) -> int:
        return hash(("socket_address", self.socket_address))


class Folder:
    def __init__(self, folder_name: str, path: Path) -> None:
        self.folder_name = folder_name
        self.path = path / folder_name
        self.for_anonymous = "anon" in folder_name
        self.for_geolocation = "geo" in folder_name

    def remove(self) -> None:
        try:
            rmtree(self.path)
        except FileNotFoundError:
            pass

    def create(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)


class ProxyScraperChecker:
    """HTTP, SOCKS4, SOCKS5 proxies scraper and checker."""

    def __init__(
        self,
        *,
        timeout: float,
        max_connections: int,
        sort_by_speed: bool,
        save_path: str,
        proxies: bool,
        proxies_anonymous: bool,
        proxies_geolocation: bool,
        proxies_geolocation_anonymous: bool,
        http_sources: Optional[Iterable[str]],
        socks4_sources: Optional[Iterable[str]],
        socks5_sources: Optional[Iterable[str]],
    ) -> None:
        """HTTP, SOCKS4, SOCKS5 proxies scraper and checker.

        Args:
            timeout: How many seconds to wait for the connection.
            max_connections: Maximum concurrent connections.
            sort_by_speed: Set to False to sort proxies alphabetically.
            save_path: Path to the folder where the proxy folders will be
                saved.
        """
        self.path = Path(save_path)
        folders_mapping = {
            "proxies": proxies,
            "proxies_anonymous": proxies_anonymous,
            "proxies_geolocation": proxies_geolocation,
            "proxies_geolocation_anonymous": proxies_geolocation_anonymous,
        }
        self.all_folders = [
            Folder(folder_name, self.path) for folder_name in folders_mapping
        ]
        self.enabled_folders = [
            folder
            for folder in self.all_folders
            if folders_mapping[folder.folder_name]
        ]
        if not self.enabled_folders:
            raise ValueError("all folders are disabled in the config")

        regex = r"(?:^|\D)(({0}\.{1}\.{1}\.{1}):{2})(?:\D|$)".format(
            r"(?:[1-9]|[1-9]\d|1\d{2}|2[0-4]\d|25[0-5])",  # 1-255
            r"(?:\d|[1-9]\d|1\d{2}|2[0-4]\d|25[0-5])",  # 0-255
            r"(?:\d|[1-9]\d{1,3}|[1-5]\d{4}|6[0-4]\d{3}"
            + r"|65[0-4]\d{2}|655[0-2]\d|6553[0-5])",  # 0-65535
        )
        self.regex = re.compile(regex)

        self.sort_by_speed = sort_by_speed
        self.timeout = timeout
        self.sources = {
            proto: (sources,)
            if isinstance(sources, str)
            else frozenset(sources)
            for proto, sources in (
                ("http", http_sources),
                ("socks4", socks4_sources),
                ("socks5", socks5_sources),
            )
            if sources
        }
        self.proxies: Dict[str, Set[Proxy]] = {
            proto: set() for proto in self.sources
        }
        self.proxies_count = {proto: 0 for proto in self.sources}
        self.sem = asyncio.Semaphore(max_connections)

    async def fetch_source(
        self, session: ClientSession, source: str, proto: str
    ) -> None:
        """Get proxies from source.

        Args:
            source: Proxy list URL.
            proto: http/socks4/socks5.
        """
        try:
            async with session.get(source.strip(), timeout=15) as r:
                text = await r.text(encoding="utf-8")
        except Exception as e:
            logging.error("%s: %s", source, e)
        else:
            for proxy in self.regex.finditer(text):
                self.proxies[proto].add(Proxy(proxy.group(1), proxy.group(2)))

    async def check_proxy(self, proxy: Proxy, proto: str) -> None:
        """Check if proxy is alive."""
        try:
            async with self.sem:
                start = perf_counter()
                async with ClientSession(
                    connector=ProxyConnector.from_url(
                        f"{proto}://{proxy.socket_address}"
                    )
                ) as session:
                    async with session.get(
                        "http://ip-api.com/json/", timeout=self.timeout
                    ) as r:
                        res = (
                            None
                            if r.status in {403, 404, 429}
                            else await r.json()
                        )
        except Exception as e:
            # Too many open files
            if isinstance(e, OSError) and e.errno == 24:
                logging.error("Please, set MAX_CONNECTIONS to lower value.")

            self.proxies[proto].remove(proxy)
        else:
            proxy.timeout = perf_counter() - start
            if res:
                proxy.update(res)

    async def fetch_all_sources(self) -> None:
        logging.info("Fetching sources")
        async with ClientSession() as session:
            coroutines = (
                self.fetch_source(session, source, proto)
                for proto, sources in self.sources.items()
                for source in sources
            )
            await asyncio.gather(*coroutines)

        # Remember total count so we could print it in the table
        for proto, proxies in self.proxies.items():
            self.proxies_count[proto] = len(proxies)

    async def check_all_proxies(self) -> None:
        logging.info(
            "Checking %s proxies",
            ", ".join(
                f"{len(proxies)} {proto.upper()}"
                for proto, proxies in self.proxies.items()
            ),
        )
        coroutines = [
            self.check_proxy(proxy, proto)
            for proto, proxies in self.proxies.items()
            for proxy in proxies
        ]
        shuffle(coroutines)
        await asyncio.gather(*coroutines)

    def save_proxies(self) -> None:
        """Delete old proxies and save new ones."""
        sorted_proxies = self.sorted_proxies.items()
        for folder in self.all_folders:
            folder.remove()
        for folder in self.enabled_folders:
            folder.create()
            for proto, proxies in sorted_proxies:
                text = "\n".join(
                    "{}{}".format(
                        proxy.socket_address,
                        proxy.geolocation if folder.for_geolocation else "",
                    )
                    for proxy in proxies
                    if (proxy.is_anonymous if folder.for_anonymous else True)
                )
                (folder.path / f"{proto}.txt").write_text(
                    text, encoding="utf-8"
                )

    async def main(self) -> None:
        await self.fetch_all_sources()
        await self.check_all_proxies()

        logging.info("Result:")
        for proto, proxies in self.proxies.items():
            working = len(proxies)
            total = self.proxies_count[proto]
            percentage = working / total * 100 if total else 0
            logging.info(
                "%s - %d/%d (%.1f%%)",
                proto.upper(),
                working,
                total,
                percentage,
            )

        self.save_proxies()
        logging.info(
            "Proxy folders have been created in the %s folder.",
            self.path.absolute(),
        )
        logging.info("Thank you for using proxy-scraper-checker :)")

    @property
    def sorted_proxies(self) -> Dict[str, List[Proxy]]:
        key = self._sorting_key
        return {
            proto: sorted(proxies, key=key)
            for proto, proxies in self.proxies.items()
        }

    @property
    def _sorting_key(
        self,
    ) -> Union[Callable[[Proxy], float], Callable[[Proxy], Tuple[int, ...]]]:
        if self.sort_by_speed:
            return lambda proxy: proxy.timeout
        return lambda proxy: tuple(
            map(int, proxy.socket_address.replace(":", ".").split("."))
        )


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    await ProxyScraperChecker(
        timeout=config.TIMEOUT,
        max_connections=config.MAX_CONNECTIONS,
        sort_by_speed=config.SORT_BY_SPEED,
        save_path=config.SAVE_PATH,
        proxies=config.PROXIES,
        proxies_anonymous=config.PROXIES_ANONYMOUS,
        proxies_geolocation=config.PROXIES_GEOLOCATION,
        proxies_geolocation_anonymous=config.PROXIES_GEOLOCATION_ANONYMOUS,
        http_sources=config.HTTP_SOURCES
        if config.HTTP and config.HTTP_SOURCES
        else None,
        socks4_sources=config.SOCKS4_SOURCES
        if config.SOCKS4 and config.SOCKS4_SOURCES
        else None,
        socks5_sources=config.SOCKS5_SOURCES
        if config.SOCKS5 and config.SOCKS5_SOURCES
        else None,
    ).main()


if __name__ == "__main__":
    asyncio.run(main())
