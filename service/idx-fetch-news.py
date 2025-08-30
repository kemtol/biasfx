#!/usr/bin/env python3
# Flexible Google News RSS fetcher (cron-friendly)
# - Output harian (WIB): news/google_rss_query_YYYY-MM-DD.csv
# - Append + dedup (by link; fallback hash judul+summary)
# - Config precedence: CLI args > ENV > defaults
# - Resolves relative paths against project root (one level above this script)

import argparse, os, sys, time, hashlib, random, json
import pandas as pd
import feedparser, requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from dateutil import tz

# ========= Path & TZ =========
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJ_ROOT  = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
WIB = tz.gettz("Asia/Jakarta")
def now_wib(): return datetime.now(WIB)
def resolve_path(p: str | None) -> str | None:
    if not p: return p
    return p if os.path.isabs(p) else os.path.join(PROJ_ROOT, p)

# ========= Helpers =========
def clean_html(s:str) -> str:
    return BeautifulSoup(s or "", "lxml").get_text(" ", strip=True)

def unwrap_gnews_link(link:str)->str:
    if "news.google." in link:
        qs = parse_qs(urlparse(link).query)
        if "url" in qs and qs["url"]:
            return qs["url"][0]
    return link

def read_lines(path:str) -> list[str]:
    if not path: return []
    path = resolve_path(path)
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

def fetch_gnews_raw(query:str, when:str="1d", lang="id", country="ID") -> pd.DataFrame:
    q = f"{query} when:{when}" if when else query
    rss = f"https://news.google.com/rss/search?q={requests.utils.quote(q)}&hl={lang}&gl={country}&ceid={country}%3A{lang}"
    parsed = feedparser.parse(rss)
    rows = []
    for e in parsed.entries:
        title   = clean_html(getattr(e, "title", ""))
        summary = clean_html(getattr(e, "summary", "") or getattr(e, "description", ""))
        link    = unwrap_gnews_link(getattr(e, "link", ""))
        ts      = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        pub     = datetime.fromtimestamp(time.mktime(ts), tz=WIB) if ts else now_wib()
        rows.append({"published_wib":pub, "source":"GoogleNews", "title":title, "summary":summary, "link":link, "q":query})
    df = pd.DataFrame(rows)
    if df.empty: 
        return df
    # batch-level dedup key
    def _key(r):
        if r["link"]: return r["link"]
        return hashlib.sha1((r["title"]+"||"+r["summary"]).encode("utf-8")).hexdigest()
    df["dedup_key"] = df.apply(_key, axis=1)
    return df.sort_values("published_wib", ascending=False).drop_duplicates("dedup_key")

def fetch_gnews(query:str, when="1d", sites=None, pause=0.4) -> pd.DataFrame:
    if not sites:
        return fetch_gnews_raw(query, when=when)
    frames=[]
    for d in sites:
        q = f"({query}) site:{d.strip()}"
        part = fetch_gnews_raw(q, when=when)
        if not part.empty: frames.append(part)
        time.sleep(pause)  # sopan antardomain
    if not frames:
        return pd.DataFrame(columns=["published_wib","source","title","summary","link","q","dedup_key"])
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values("published_wib", ascending=False).drop_duplicates("dedup_key")

def expand_templates(q:str, tickers, companies, invmgrs, tokohs, years) -> list[str]:
    out=[q]
    def rep_multi(items, token):
        new=[]
        for s in out:
            if f"[{token}]" in s:
                lst = items[:20] if items else []   # batasi spy ringan
                new.extend([s.replace(f"[{token}]", itm) for itm in (lst or [""])])
            else:
                new.append(s)
        return new
    out = rep_multi(tickers,   "kode emiten")
    out = rep_multi(companies, "nama perusahaan")
    out = rep_multi(invmgrs,   "nama manajer investasi")
    out = rep_multi(tokohs,    "nama tokoh investor")
    out = rep_multi(years,     "tahun")
    return [" ".join(s.split()) for s in out]

def str2list(s: str | None) -> list[str]:
    if not s: return []
    if os.path.exists(resolve_path(s)):   # kalau dia file, baca baris
        return read_lines(s)
    # kalau bukan file → comma separated
    return [x.strip() for x in s.split(",") if x.strip()]

# ========= Main =========
def main():
    ap = argparse.ArgumentParser()
    # sumber keyword & placeholders
    ap.add_argument("--keywords",   default=os.getenv("NEWS_KEYWORDS",   "config/KEYWORDS.txt"), help="File kata kunci (1/baris)")
    ap.add_argument("--tickers",    default=os.getenv("NEWS_TICKERS",    "config/TICKERS.txt"))
    ap.add_argument("--companies",  default=os.getenv("NEWS_COMPANIES",  "config/COMPANIES.txt"))
    ap.add_argument("--invmanagers",default=os.getenv("NEWS_INVMGRS",    "config/INV_MANAGERS.txt"))
    ap.add_argument("--investors",  default=os.getenv("NEWS_INVESTORS",  "config/TOKOH_INVESTOR.txt"))
    ap.add_argument("--years",      default=os.getenv("NEWS_YEARS",      "config/YEARS.txt"))
    # pemilihan keyword
    ap.add_argument("--query", default=os.getenv("NEWS_QUERY",""), help="Override: satu kata kunci langsung (abaikan --idx & --keywords)")
    ap.add_argument("--idx", type=int, default=None, help="Index kata kunci (rotasi). Jika kosong → dihitung otomatis dari waktu.")
    ap.add_argument("--rotate-mins", type=int, default=int(os.getenv("NEWS_ROTATE_MINS", "15")),
                    help="Durasi rotasi menit utk auto-idx bila --idx tidak diberikan (default 15).")
    # pencarian
    ap.add_argument("--when",   default=os.getenv("NEWS_WHEN","1d"), help="Window waktu GNews: 3h/1d/7d...")
    ap.add_argument("--lang",   default=os.getenv("NEWS_LANG","id"))
    ap.add_argument("--country",default=os.getenv("NEWS_COUNTRY","ID"))
    ap.add_argument("--sites",  default=os.getenv("NEWS_SITES",""), help="Comma-separated domains ATAU path file daftar domain")
    ap.add_argument("--pause",  type=float, default=float(os.getenv("NEWS_SITE_PAUSE","0.4")), help="Jeda antar-domain (detik)")
    # output
    ap.add_argument("--outdir", default=resolve_path(os.getenv("NEWS_OUTDIR","news")), help="Folder output harian (relatif ke rootproject)")
    ap.add_argument("--name",   default=os.getenv("NEWS_PREFIX","news"), help="Prefix nama file harian")
    # filter & limit
    ap.add_argument("--minhour", type=int, default=int(os.getenv("NEWS_MINHOUR","0")), help="Filter umur maks item (jam). 0=off")
    ap.add_argument("--limit",   type=int, default=int(os.getenv("NEWS_LIMIT","24")), help="Batas jumlah query hasil ekspansi per run")
    # misc
    ap.add_argument("--print", dest="do_print", action="store_true")
    args = ap.parse_args()

    # resolve paths
    outdir = resolve_path(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    # pilih keyword dasar
    if args.query:
        base = args.query.strip()
    else:
        kwords = read_lines(args.keywords)
        if not kwords:
            print('KEYWORDS kosong. Isi file atau gunakan --query "kata kunci".')
            sys.exit(1)
        if args.idx is None:
            epoch = int(time.time())
            step  = max(args.rotate_mins, 1) * 60
            auto_idx = epoch // step
            args.idx = auto_idx
        base = kwords[args.idx % len(kwords)]

    # placeholder lists
    tickers   = read_lines(args.tickers)
    companies = read_lines(args.companies)
    invmgrs   = read_lines(args.invmanagers)
    tokohs    = read_lines(args.investors)
    years     = read_lines(args.years)

    # expand templates → list queries
    queries = expand_templates(base, tickers, companies, invmgrs, tokohs, years)
    if len(queries) > args.limit:
        random.shuffle(queries)
        queries = queries[:args.limit]

    # sites: bisa file atau comma-separated
    sites_list = str2list(args.sites)

    # fetch per-query
    frames=[]
    for q in queries:
        df = fetch_gnews(q, when=args.when, sites=sites_list or None, pause=args.pause)
        if args.minhour and not df.empty:
            cutoff = now_wib() - pd.Timedelta(hours=args.minhour)
            df = df[df["published_wib"] >= cutoff]
        if not df.empty:
            frames.append(df)

    if frames:
        batch = pd.concat(frames, ignore_index=True)
        batch = batch.sort_values("published_wib", ascending=False).drop_duplicates("dedup_key")
    else:
        batch = pd.DataFrame(columns=["published_wib","source","title","summary","link","q","dedup_key"])

    # simpan harian
    out_path = os.path.join(outdir, f"{args.name}_{now_wib().strftime('%Y-%m-%d')}.csv")

    # gabung + dedup kuat
    if os.path.exists(out_path):
        try:
            old = pd.read_csv(out_path, parse_dates=["published_wib"])
        except Exception:
            old = pd.read_csv(out_path)
        merged = pd.concat([old, batch], ignore_index=True)
        if "dedup_key" not in merged.columns:
            merged["dedup_key"] = merged["link"].where(merged["link"].astype(bool),
                                                       merged["title"].fillna("")+"||"+merged["summary"].fillna(""))
        use_key = merged["link"].where(merged["link"].astype(bool), merged["dedup_key"])
        merged = (merged.assign(_dupe_key=use_key)
                        .sort_values("published_wib", ascending=False)
                        .drop_duplicates("_dupe_key")
                        .drop(columns=["_dupe_key"]))
    else:
        merged = batch

    if "published_wib" in merged.columns:
        merged = merged.sort_values("published_wib", ascending=False)

    merged.to_csv(out_path, index=False)
    print(f"[OK] {len(batch)} baru | total hari ini: {len(merged)} → {out_path}")

    if args.do_print:
        try:
            from IPython.display import display
            display(merged.head(15))
        except Exception:
            print(merged.head(15).to_string(index=False))

if __name__ == "__main__":
    main()
