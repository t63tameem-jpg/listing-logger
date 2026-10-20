"""H10 data capture (see PREREGISTRATION_2.md): LOGS ONLY. Never trades, never uses API keys or accounts.

Polls public, unauthenticated GET endpoints for new exchange listing announcements and new spot markets that mention
the watched meme coins, records when this machine first saw each one, and takes top-of-book bid/ask snapshots at
fixed offsets afterwards so a future backtest can use real executable quotes instead of last-trade prices.

Outputs (data/meme_listings/): events.csv, quotes.csv, state.json. The first run only baselines what already exists.
Usage: .venv/bin/python listing_logger.py --once          # one poll, then exit
       .venv/bin/python listing_logger.py --max-minutes 330   # run ~5.5 h then exit (cloud jobs)
       .venv/bin/python listing_logger.py --interval 20   # run until stopped (Ctrl-C)
"""
import argparse
import csv
import json
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

OUT = Path(__file__).parent / "data" / "meme_listings"
WATCH = {"DOGE": r"doge(coin)?", "SHIB": r"shib(a)?(\s*inu)?", "PEPE": r"pepe", "WIF": r"wif|dogwifhat", "BONK": r"bonk"}
OFFSETS = (0, 60, 300, 1800, 1860)   # seconds after first sight; H10 buys at +60 and sells 30 min later at +1860
HEADERS = {"User-Agent": "mission-2k-listing-logger/1.0 (read-only research)"}
SOURCES = {
    "binance_announce": "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
                        "?type=1&catalogId=48&pageNo=1&pageSize=20",
    "upbit_notice": "https://api-manager.upbit.com/api/v1/announcements?os=web&page=1&per_page=30&category=trade",
    "binance_symbols": "https://data-api.binance.vision/api/v3/ticker/price",
    "coinbase_products": "https://api.exchange.coinbase.com/products",
}
EVENT_FIELDS = ["event_id", "received_utc", "source", "item_id", "title", "published", "token", "kind"]
QUOTE_FIELDS = ["event_id", "offset_s", "due_utc", "requested_utc", "taken_utc", "venue", "symbol", "bid", "ask", "mid", "error"]


def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def get_json(url, timeout=10):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.json()


def tokens_in(text):
    return [t for t, pat in WATCH.items() if re.search(rf"(?<![A-Za-z0-9])({pat})(?![A-Za-z0-9])", text or "", re.I)]


def kind_of(title):
    t = (title or "").lower()
    if "delist" in t or "remov" in t:
        return "delisting"
    if "will list" in t or "listing" in t or "lists" in t or "list " in t or "new" in t:
        return "listing"
    return "other"


# ---- parsers: return [(item_id, title, published)] ----
class BadResponse(Exception):
    pass


def _need_str(v, what, allow_int=False):
    ok = isinstance(v, str) or (allow_int and isinstance(v, int) and not isinstance(v, bool))
    if not ok or not str(v).strip():
        raise BadResponse(f"missing or mistyped {what}")
    return str(v)


def _dict(v, what):
    if not isinstance(v, dict):
        raise BadResponse(f"{what} is not an object")
    return v


def parse_binance_announce(js):
    cats = _dict(_dict(js, "binance response").get("data"), "binance data").get("catalogs")
    if not isinstance(cats, list) or not cats or str(js.get("code")) != "000000":
        raise BadResponse("binance announcement schema")
    out = []
    for cat in cats:
        arts = cat.get("articles") if isinstance(cat, dict) else None
        if not isinstance(arts, list):
            raise BadResponse("binance catalog without articles list")
        for a in arts:
            if not isinstance(a, dict):
                raise BadResponse("binance article not an object")
            out.append((_need_str(a.get("code") or a.get("id"), "article id", allow_int=True),
                        _need_str(a.get("title"), "title"),
                        str(a.get("releaseDate", ""))))
    if not out:
        raise BadResponse("binance announcements empty")
    return out


def parse_upbit_notice(js):
    notices = _dict(_dict(js, "upbit response").get("data"), "upbit data").get("notices")
    if not isinstance(notices, list) or not notices or js.get("success") is False:
        raise BadResponse("upbit notice schema")
    out = []
    for n in notices:
        if not isinstance(n, dict):
            raise BadResponse("upbit notice not an object")
        out.append((_need_str(n.get("id"), "notice id", allow_int=True), _need_str(n.get("title"), "title"), str(n.get("listed_at", ""))))
    return out


def _ids(js, key, what):
    if not isinstance(js, list) or not js:
        raise BadResponse(f"{what} schema")
    return sorted({_need_str(x.get(key) if isinstance(x, dict) else None, f"{what} {key}") for x in js})


def parse_binance_symbols(js):
    return _ids(js, "symbol", "binance symbols")


def parse_coinbase_products(js):
    return _ids(js, "id", "coinbase products")


def base_of(symbol, quotes=("USDT", "USDC", "USD", "FDUSD", "BTC", "EUR", "-USD", "-USDC", "-EUR", "-BTC")):
    for q in sorted(quotes, key=len, reverse=True):
        if symbol.endswith(q):
            return symbol[: -len(q)].rstrip("-")
    return symbol


# ---- quotes ----
def valid_quote(bid, ask):
    if not (math.isfinite(bid) and math.isfinite(ask) and bid > 0 and ask > 0 and bid <= ask):
        raise ValueError(f"invalid or crossed quote bid={bid} ask={ask}")
    return bid, ask


def quote_binance(sym):
    j = get_json(f"https://data-api.binance.vision/api/v3/ticker/bookTicker?symbol={sym}USDT")
    return valid_quote(float(j["bidPrice"]), float(j["askPrice"]))


def quote_coinbase(sym):
    j = get_json(f"https://api.exchange.coinbase.com/products/{sym}-USD/ticker")
    return valid_quote(float(j["bid"]), float(j["ask"]))


VENUES = {"binance": quote_binance, "coinbase": quote_coinbase}


class Logger:
    def __init__(self, out=OUT, fetch=get_json, venues=None, clock=now):
        self.out, self.fetch, self.clock = Path(out), fetch, clock
        self.venues = venues or VENUES
        self.out.mkdir(parents=True, exist_ok=True)
        self.state_path = self.out / "state.json"
        # CSV keys already on disk: makes appends idempotent if the process died between a write and save()
        self.keys = {"events.csv": self._csv_keys("events.csv", ("event_id",)),
                     "quotes.csv": self._csv_keys("quotes.csv", ("event_id", "offset_s", "venue"))}
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {
            "seen": {}, "symbols": {}, "pending": [], "baselined": []}
        self._reconcile_schedules()

    # -- persistence --
    def _csv_keys(self, name, cols):
        path = self.out / name
        if not path.exists():
            return set()
        with path.open(newline="") as f:
            return {tuple(r[c] for c in cols) for r in csv.DictReader(f)}

    def _append(self, name, fields, row):
        key = tuple(str(row[c]) for c in (("event_id",) if name == "events.csv" else ("event_id", "offset_s", "venue")))
        if key in self.keys[name]:
            return False
        self.keys[name].add(key)
        path = self.out / name
        new = not path.exists()
        if not new:
            with path.open(newline="") as f:
                header = next(csv.reader(f), [])
            if header != fields:                    # never write rows under a different schema
                raise RuntimeError(f"{path} has header {header}, expected {fields}; move or migrate the file")
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            if new:
                w.writeheader()
            w.writerow(row)
        return True

    def save(self):
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state))
        tmp.replace(self.state_path)

    def _reconcile_schedules(self):
        """After a crash between a CSV append and save(), rebuild snapshot schedules from the persisted events,
        keeping original due times and skipping offsets that already have a quote row."""
        path = self.out / "events.csv"
        if not path.exists():
            return
        have = set(self.keys["quotes.csv"])
        queued = {(p["event_id"], str(p["offset"])) for p in self.state["pending"]}
        with path.open(newline="") as f:
            for r in csv.DictReader(f):
                eid = r["event_id"]
                if eid not in self.state["seen"].get("_events", []):
                    self.state["seen"].setdefault("_events", []).append(eid)
                t0 = datetime.strptime(r["received_utc"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()
                for off in OFFSETS:
                    missing = any((eid, str(off), v) not in have for v in self.venues)
                    if missing and (eid, str(off)) not in queued:
                        self.state["pending"].append(dict(event_id=eid, token=r["token"], offset=off, due=t0 + off))

    # -- event detection --
    def _emit(self, source, item_id, title, published, token, kind):
        t = self.clock()
        eid = f"{source}:{item_id}:{token}"
        if any(p["event_id"] == eid for p in self.state["pending"]) or eid in self.state["seen"].get("_events", []):
            return
        self.state["seen"].setdefault("_events", []).append(eid)
        if (eid,) in self.keys["events.csv"]:
            return                                  # logged before a crash; its snapshot schedule is not recoverable
        self._append("events.csv", EVENT_FIELDS, dict(event_id=eid, received_utc=iso(t), source=source, item_id=item_id,
                                                      title=title, published=published, token=token, kind=kind))
        for off in OFFSETS:
            self.state["pending"].append(dict(event_id=eid, token=token, offset=off,
                                              due=(t.timestamp() + off)))

    def poll_announcements(self, source, parser):
        try:
            items = parser(self.fetch(SOURCES[source]))
        except Exception as e:                      # network or schema error: log and carry on
            print(f"[{source}] error: {e}")
            return
        seen = set(self.state["seen"].get(source, []))
        first = source not in self.state["baselined"]
        for item_id, title, published in items:
            if item_id in seen:
                continue
            seen.add(item_id)
            if first:
                continue                            # baseline: pre-existing items are not events
            for tok in tokens_in(title):
                self._emit(source, item_id, title, published, tok, kind_of(title))
        self.state["seen"][source] = sorted(seen)
        if first:
            self.state["baselined"].append(source)

    def poll_symbols(self, source, parser):
        try:
            symbols = parser(self.fetch(SOURCES[source]))
        except Exception as e:
            print(f"[{source}] error: {e}")
            return
        prev = set(self.state["symbols"].get(source, []))
        first = source not in self.state["baselined"]
        if not first:
            for s in symbols:
                if s not in prev and base_of(s) in WATCH:
                    self._emit(source, s, f"new market {s}", "", base_of(s), "new_market")
        self.state["symbols"][source] = symbols
        if first:
            self.state["baselined"].append(source)

    # -- snapshots --
    def take_due_snapshots(self):
        t = self.clock().timestamp()
        keep = []
        for p in self.state["pending"]:
            if p["due"] > t:
                keep.append(p)
                continue
            for venue, fn in self.venues.items():
                if (p["event_id"], str(p["offset"]), venue) in self.keys["quotes.csv"]:
                    continue                        # already recorded before a crash
                row = dict(event_id=p["event_id"], offset_s=p["offset"], due_utc=iso(datetime.fromtimestamp(p["due"], timezone.utc)),
                           requested_utc=iso(self.clock()), taken_utc="", venue=venue, symbol=p["token"], bid="", ask="",
                           mid="", error="")
                try:
                    bid, ask = fn(p["token"])
                    row.update(bid=bid, ask=ask, mid=(bid + ask) / 2)
                except Exception as e:
                    row["error"] = str(e)[:120]
                row["taken_utc"] = iso(self.clock())    # when the answer was actually in hand (after the request)
                self._append("quotes.csv", QUOTE_FIELDS, row)
        self.state["pending"] = keep

    def step(self):
        self.poll_announcements("binance_announce", parse_binance_announce)
        self.poll_announcements("upbit_notice", parse_upbit_notice)
        self.poll_symbols("binance_symbols", parse_binance_symbols)
        self.poll_symbols("coinbase_products", parse_coinbase_products)
        self.take_due_snapshots()
        self.save()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval", type=float, default=20.0)
    ap.add_argument("--max-minutes", type=float, default=None, help="stop after this long (for scheduled cloud jobs)")
    args = ap.parse_args()
    lg = Logger()
    end = time.time() + args.max_minutes * 60 if args.max_minutes else None
    while True:
        lg.step()
        if args.once or (end and time.time() + args.interval >= end):
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
