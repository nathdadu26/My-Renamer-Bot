"""
health_check.py
───────────────
Koyeb free tier ke liye 2 kaam karta hai:
  1. HTTP server  → Koyeb ka healthcheck pass karne ke liye (GET /)
  2. Self-pinger  → Har 5 minute mein apni hi service ko ping karta hai
                    taaki Koyeb free tier mein service sleep na ho

Usage: bot ke saath ek saath chalao (main.py se import karo ya alag process mein)
"""

import asyncio
import logging
import os

from aiohttp import web, ClientSession, ClientTimeout

logger = logging.getLogger(__name__)

PORT       = int(os.getenv("PORT", "8000"))          # Koyeb PORT env var
PUBLIC_URL = os.getenv("PUBLIC_URL", "")             # apni service ka URL, e.g. https://xxx.koyeb.app
PING_EVERY = int(os.getenv("PING_INTERVAL", "300"))  # seconds (default 5 min)

# ── Health endpoint ──────────────────────────────────────────────────────────

async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "MediaBot"})

async def root_handler(request: web.Request) -> web.Response:
    return web.Response(text="MediaBot is running ✅")

# ── Self-pinger (keep-alive) ─────────────────────────────────────────────────

async def self_pinger():
    """
    Agar PUBLIC_URL set hai to har PING_EVERY seconds mein apni
    / route ko ping karta hai — Koyeb free tier service ko jaag rakhta hai.
    """
    if not PUBLIC_URL:
        logger.warning("PUBLIC_URL not set — self-pinger disabled.")
        return

    url     = PUBLIC_URL.rstrip("/") + "/"
    timeout = ClientTimeout(total=10)

    while True:
        await asyncio.sleep(PING_EVERY)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    logger.info(f"[keep-alive] ping {url} → {resp.status}")
        except Exception as e:
            logger.warning(f"[keep-alive] ping failed: {e}")

# ── App factory ──────────────────────────────────────────────────────────────

def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",       root_handler)
    app.router.add_get("/health", health_handler)
    return app

# ── Start (call this from your main entry point) ─────────────────────────────

async def start_health_server():
    """
    Asyncio-friendly start — bot ke event loop mein add karo:

        asyncio.ensure_future(start_health_server())

    ya directly:

        await start_health_server()   # (yeh block karta hai, isliye task banao)
    """
    app    = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health server started on port {PORT}")

    # keep-alive pinger background mein
    asyncio.ensure_future(self_pinger())

# ── Standalone run (agar alag process chahiye) ───────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    asyncio.run(start_health_server())
