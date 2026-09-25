"""Lazy Telegram fallback-IP discovery producer."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
from collections.abc import Callable, Iterable

import httpx

logger = logging.getLogger(__name__)


def _resolve_system_dns(telegram_api_host: str) -> set[str]:
    """Return the IPv4 addresses that the OS resolver gives for the Telegram API."""
    try:
        results = socket.getaddrinfo(telegram_api_host, 443, socket.AF_INET)
        return {addr[4][0] for addr in results}
    except Exception:
        return set()


async def _query_doh_provider(client: httpx.AsyncClient, provider: dict) -> list[str]:
    """Query one DoH provider and return A-record IPs."""
    try:
        resp = await client.get(provider["url"], params=provider["params"], headers=provider["headers"])
        resp.raise_for_status()
        data = resp.json()
        ips: list[str] = []
        for answer in data.get("Answer", []):
            if answer.get("type") != 1:
                continue
            raw = answer.get("data", "").strip()
            try:
                ipaddress.ip_address(raw)
            except ValueError:
                continue
            ips.append(raw)
        return ips
    except Exception as exc:
        logger.debug("DoH query to %s failed: %s", provider["url"], exc)
        return []


async def discover_fallback_ips(
    *,
    doh_timeout: float,
    doh_providers: list[dict],
    seed_fallback_ips: list[str],
    normalize_fallback_ips: Callable[[Iterable[str]], list[str]],
    telegram_api_host: str,
) -> list[str]:
    """Resolve Telegram API IPv4 via DoH, using seed addresses if no usable answers arrive."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(doh_timeout)) as client:
        system_dns_task = asyncio.ensure_future(asyncio.to_thread(_resolve_system_dns, telegram_api_host))
        results = await asyncio.gather(
            *[_query_doh_provider(client, provider) for provider in doh_providers], return_exceptions=True)
    system_ips: set[str] = set()
    try:
        system_result = await asyncio.wait_for(system_dns_task, timeout=doh_timeout)
        if isinstance(system_result, set):
            system_ips = system_result
    except Exception:
        logger.debug("System-DNS resolution for %s did not complete in time", telegram_api_host)
    doh_ips = [ip for result in results if isinstance(result, list) for ip in result]
    validated = normalize_fallback_ips(list(dict.fromkeys(doh_ips)))
    if validated:
        logger.debug("Discovered Telegram fallback IPs via DoH: %s", ", ".join(validated))
        return validated
    logger.info(
        "DoH discovery yielded no usable IPs (system DNS: %s); using seed fallback IPs %s",
        ", ".join(system_ips) or "unknown", ", ".join(seed_fallback_ips))
    return list(seed_fallback_ips)
