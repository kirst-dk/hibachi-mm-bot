from __future__ import annotations
import os, logging, time, signal, sys, asyncio
from logging.handlers import RotatingFileHandler
from pathlib import Path

from hibachi_client import HibachiRest
from hibachi_mm_engine import HibachiMarketMakerEngine
from env_config import load_env_config, validate_config

shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    print("\n\nShutdown requested...")
    shutdown_requested = True


def setup_logging(log_dir: str, level: str = "INFO"):
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    fh = RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)


def step_with_retry(mm: HibachiMarketMakerEngine, max_retries: int = 3) -> bool:
    import requests

    for attempt in range(max_retries):
        try:
            mm.step()
            return True
        except requests.exceptions.ConnectionError as e:
            logging.warning("Connection error (attempt %d/%d): %s",
                            attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            continue
        except Exception as e:
            logging.error("Step error: %s", e, exc_info=True)
            return False

    logging.error("Step failed after %d retries", max_retries)
    return False


# ─── REST mode ─────────────────────────────────────────────────────────

def run_rest_mode(cfg: dict):
    """Run MM bot in REST polling mode (original behavior, multi-pair aware)."""
    global shutdown_requested
    log = logging.getLogger("main")
    api = cfg["api"]
    bot = cfg["bot"]
    symbols = bot.get("symbols", [bot["symbol"]])
    num_symbols = len(symbols)

    log.info("Initializing Hibachi REST client...")
    rest = HibachiRest(
        api_url=api["apiUrl"],
        data_api_url=api["dataApiUrl"],
        api_key=api["apiKey"],
        account_id=api["accountId"],
        private_key=api["privateKey"]
    )

    engines: list[HibachiMarketMakerEngine] = []
    for symbol in symbols:
        sym_cfg = dict(bot)
        sym_cfg["symbol"] = symbol
        if num_symbols > 1:
            sym_cfg["invBudgetPct"] = bot["invBudgetPct"] / num_symbols

        mm = HibachiMarketMakerEngine(rest, sym_cfg, cfg["logging"]["dir"])
        try:
            log.info("Bootstrapping %s...", symbol)
            mm.bootstrap_markets()
            mm.bootstrap_atr()
            mm.bootstrap_equity_and_pos()
            engines.append(mm)
        except Exception as e:
            log.error("Bootstrap failed for %s: %s", symbol, e, exc_info=True)
            sys.exit(1)

    log.info("=" * 70)
    log.info("Bot RUNNING (%d symbol(s), REST mode) | Ctrl+C to stop", num_symbols)
    log.info("=" * 70)

    loop_interval = 5.0
    last_step = 0

    try:
        while not shutdown_requested:
            now = time.time()
            if now - last_step >= loop_interval:
                for mm in engines:
                    step_with_retry(mm)
                last_step = now
            time.sleep(0.5)
    except KeyboardInterrupt:
        log.info("Keyboard interrupt")
    finally:
        log.info("Shutting down...")
        for mm in engines:
            try:
                mm._cancel_both()
            except Exception:
                pass
        time.sleep(1)
        for mm in engines:
            try:
                mm._force_equity_update()
                log.info("[%s] Final equity: $%.2f | Position: %.4f",
                         mm.symbol, mm.state.equity_usd, mm.state.pos_qty)
                if mm.state.fills_count > 0:
                    log.info("[%s] Session: %d fills | vol=$%.0f | pnl=$%.4f",
                             mm.symbol, mm.state.fills_count,
                             mm.state.total_volume, mm.state.realized_pnl)
            except Exception as e:
                log.error("Shutdown error for %s: %s", mm.symbol, e)
        log.info("Goodbye!")


# ─── WebSocket mode ────────────────────────────────────────────────────

async def run_ws_mode(cfg: dict):
    """Run MM bot in WebSocket mode (lower latency, real-time data)."""
    global shutdown_requested
    from hibachi_ws_client import HibachiWS
    from hibachi_ws_engine import HibachiWSMarketMakerEngine

    log = logging.getLogger("main")
    api = cfg["api"]
    bot = cfg["bot"]
    symbols = bot.get("symbols", [bot["symbol"]])
    num_symbols = len(symbols)

    log.info("Initializing Hibachi REST client (for bootstrap)...")
    rest = HibachiRest(
        api_url=api["apiUrl"],
        data_api_url=api["dataApiUrl"],
        api_key=api["apiKey"],
        account_id=api["accountId"],
        private_key=api["privateKey"]
    )

    log.info("Initializing Hibachi WebSocket client...")
    ws = HibachiWS(
        api_url=api["apiUrl"],
        api_key=api["apiKey"],
        account_id=api["accountId"],
        private_key=api["privateKey"],
        public_key=api.get("publicKey", ""),
        data_api_url=api["dataApiUrl"]
    )

    # Connect WebSocket channels
    try:
        await ws.connect_market()
        await ws.connect_trade()
        await ws.connect_account()
    except Exception as e:
        log.error("WebSocket connection failed: %s", e, exc_info=True)
        log.error("Falling back to REST mode...")
        run_rest_mode(cfg)
        return

    # Enable cancel-on-disconnect for safety
    try:
        await ws.enable_cancel_on_disconnect()
        log.info("Cancel-on-disconnect enabled")
    except Exception as e:
        log.warning("Could not enable cancel-on-disconnect: %s", e)

    # Create engines for each symbol
    engines: list[HibachiWSMarketMakerEngine] = []
    for symbol in symbols:
        sym_cfg = dict(bot)
        sym_cfg["symbol"] = symbol
        if num_symbols > 1:
            sym_cfg["invBudgetPct"] = bot["invBudgetPct"] / num_symbols

        engine = HibachiWSMarketMakerEngine(ws, rest, sym_cfg, cfg["logging"]["dir"])
        engines.append(engine)

    log.info("=" * 70)
    log.info("Bot RUNNING (%d symbol(s), WebSocket mode) | Ctrl+C to stop",
             num_symbols)
    log.info("=" * 70)

    # Run all engines concurrently and stop them when shutdown is requested.
    tasks = [asyncio.create_task(engine.run()) for engine in engines]
    try:
        while not shutdown_requested:
            # If any engine crashed, stop all and surface the error in logs.
            done = [t for t in tasks if t.done()]
            if done:
                for t in done:
                    exc = t.exception()
                    if exc is not None:
                        log.error("WS engine task failed: %s", exc, exc_info=True)
                break
            await asyncio.sleep(0.2)
    except asyncio.CancelledError:
        log.info("Tasks cancelled")
    except KeyboardInterrupt:
        log.info("Keyboard interrupt")
    finally:
        log.info("Shutting down WS engines...")
        for engine in engines:
            engine.request_shutdown()

        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10)
        except asyncio.TimeoutError:
            log.warning("WS engines did not stop in time, cancelling tasks...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            await ws.cancel_all_orders()
        except Exception:
            pass
        await ws.disconnect()
        log.info("Goodbye!")


# ─── Entry point ────────────────────────────────────────────────────────

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not os.path.exists('.env'):
        print("ERROR: .env file not found!")
        print("Run: cp .env.example .env")
        print("Then edit .env with your API keys")
        sys.exit(1)

    print("Loading configuration...")
    cfg = load_env_config()

    try:
        validate_config(cfg)
    except AssertionError as e:
        print(f"ERROR: Invalid configuration: {e}")
        sys.exit(1)

    setup_logging(cfg["logging"]["dir"], cfg["logging"]["level"])
    log = logging.getLogger("main")

    log.info("=" * 70)
    log.info("Hibachi Market Maker Bot")
    log.info("=" * 70)

    symbols = cfg["bot"].get("symbols", [cfg["bot"]["symbol"]])
    log.info("Symbols: %s", ", ".join(symbols))
    log.info("Base order: %.1f%% | Inv budget: %.1f%%",
             cfg['bot']['baseOrderPct'], cfg['bot']['invBudgetPct'])

    use_ws = cfg["api"].get("useWebsocket", False)
    if use_ws:
        log.info("Mode: WebSocket (low-latency)")
        asyncio.run(run_ws_mode(cfg))
    else:
        log.info("Mode: REST (polling)")
        run_rest_mode(cfg)


if __name__ == "__main__":
    main()