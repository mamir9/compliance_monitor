#check if regulation id extraction is properly supported
#by BaseSpider for this 
#Is the date extraction format supported?

import os
import re
import json
import hashlib
import scrapy
from datetime import datetime, timedelta
from urllib.parse import urlencode, urlparse, parse_qs

from spiders import BaseSpider  # adjust import to your project
from extensions import db                  # adjust import
from models import Regulation, PageHash  # adjust import
import logging

logger = logging.getLogger(__name__)


class PakistanCodeSpider(BaseSpider):
    name = "pakistan_code_monitor"
    allowed_domains = ["pakistancode.gov.pk", "www.pakistancode.gov.pk"]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 2,
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": True,
        "RETRY_TIMES": 3,
        "HTTPERROR_ALLOWED_CODES": [500, 403],
    }

    HOME_URL = "https://pakistancode.gov.pk/english/index.php"
    CHRONO_ENDPOINT = "https://pakistancode.gov.pk/english/LGu0xBD.php"
    ORDINANCES_ENDPOINT = "https://pakistancode.gov.pk/english/KmTY5e.php"

    handle_httpstatus_list = [500, 403]

    def __init__(
        self,
        flask_app=None,
        test_mode=False,
        year=None,
        actions=None,
        max_pages=5,
        recency_days=365,
        *args,
        **kwargs
    ):
        super().__init__(flask_app=flask_app, *args, **kwargs)
        self.test_mode = test_mode
        self.year = int(year) if year else datetime.now().year
        self.actions = actions.split(",") if isinstance(actions, str) else (actions or ["inactive"])
        self.max_pages = 1 if test_mode else int(max_pages)
        self.recency_days = int(recency_days)

    # ----------------------------
    # Entry / Warm-up
    # ----------------------------
    def start_requests(self):
        # Warm-up request to get cookies/session and look more "browser-like"
        yield scrapy.Request(
            self.HOME_URL,
            callback=self.after_home,
            meta={"cookiejar": "pakcode"},
            headers=self._default_headers(referer="https://pakistancode.gov.pk/"),
            dont_filter=True,
        )

    def after_home(self, response):
        # 1) Chronological feed(s)
        logger.info("HOME status=%s url=%s", response.status, response.url)
        logger.info("HOME server=%s", response.headers.get(b"Server"))
        logger.info("HOME set-cookie=%s", response.headers.getlist(b"Set-Cookie"))
        logger.info("HOME first400=%r", response.text[:400])

        with open("/tmp/pakcode_home_debug.html", "wb") as f:
            f.write(response.body)
        for action in self.actions:
            url = self._chrono_url(year=self.year, action=action, page=1)
            yield scrapy.Request(
                url,
                callback=self.parse_chrono,
                errback=self.errback_log,
                meta={
                    "cookiejar": "pakcode",
                    "feed_kind": "chrono",
                    "year": self.year,
                    "action": action,
                    "page": 1,
                },
                headers=self._default_headers(referer=self.HOME_URL),
                dont_filter=True,
            )

        # 2) Latest ordinances feed
        yield scrapy.Request(
            self.ORDINANCES_ENDPOINT,
            callback=self.parse_ordinances_list,
            errback=self.errback_log,
            meta={"cookiejar": "pakcode", "feed_kind": "ordinances"},
            headers=self._default_headers(referer=self.HOME_URL),
            dont_filter=True,
        )

    # ----------------------------
    # Chronological: list parsing + pagination
    # ----------------------------
    def parse_chrono(self, response):
        year = response.meta["year"]
        action = response.meta["action"]
        page = response.meta["page"]

        rows = self._extract_rows(response)

        # Stop conditions
        if not rows:
            logger.info(f"PakistanCode chrono: no rows (year={year}, action={action}, page={page})")
            return

        # Hash only on page=1 (cheap “did anything change?” signal)
        if page == 1:
            stable_ids = [r.get("detail_url") or r.get("title") or "" for r in rows]
            current_hash = self.hash_urls([s for s in stable_ids if s])

            hash_key = f"PAKCODE:CHRONO:{year}:{action}:PAGE1"
            stored_hash = self.get_stored_hash(hash_key)
            if stored_hash and stored_hash == current_hash:
                logger.info(f"PakistanCode chrono unchanged (year={year}, action={action}) - skipping")
                return

            # changed -> store new hash and continue
            self.store_hash(hash_key, current_hash)

        # Process rows (follow detail pages)
        for r in rows:
            yield from self._handle_list_row(r, response, default_doc_type="Law")

        # If changed and not test_mode: crawl more pages to catch items shifted off page 1
        if page < self.max_pages:
            next_page = page + 1
            next_url = self._chrono_url(year=year, action=action, page=next_page)
            yield scrapy.Request(
                next_url,
                callback=self.parse_chrono,
                errback=self.errback_log,
                meta={**response.meta, "page": next_page},
                headers=self._default_headers(referer=response.url),
                dont_filter=True,
            )

    # ----------------------------
    # Ordinances: list parsing (usually no pagination)
    # ----------------------------
    def parse_ordinances_list(self, response):
        rows = self._extract_rows(response)

        if not rows:
            logger.warning("PakistanCode ordinances: no rows found (page may be blocked or structure changed).")
            return

        stable_ids = [r.get("detail_url") or r.get("title") or "" for r in rows]
        current_hash = self.hash_urls([s for s in stable_ids if s])

        hash_key = "PAKCODE:ORDINANCES:PAGE1"
        stored_hash = self.get_stored_hash(hash_key)
        if stored_hash and stored_hash == current_hash:
            logger.info("PakistanCode ordinances unchanged - skipping")
            return

        self.store_hash(hash_key, current_hash)

        for r in rows:
            yield from self._handle_list_row(r, response, default_doc_type="Ordinance")

    # ----------------------------
    # Detail page -> find PDF -> save
    # ----------------------------
    def _handle_list_row(self, r, list_response, default_doc_type="Law"):
        title = (r.get("title") or "").strip()
        detail_url = r.get("detail_url")
        category = (r.get("category") or "").strip()
        ref_number = (r.get("reference_number") or "").strip()
        issue_date = r.get("issue_date")

        if not detail_url:
            # fallback if no link found
            return

        # Fallback ref if missing
        if not ref_number:
            ref_number = self.generate_fallback_ref(detail_url or title)

        regulation_id = self.generate_regulation_id("PAKCODE", ref_number, issue_date)

        # If already exists, skip detail fetch
        if self.regulation_exists(regulation_id):
            return

        yield scrapy.Request(
            detail_url,
            callback=self.parse_detail,
            errback=self.errback_log,
            meta={
                "cookiejar": list_response.meta.get("cookiejar"),
                "regulation_id": regulation_id,
                "source": "PAKCODE",
                "page_url": detail_url,
                "reference_number": ref_number,
                "title": title[:200] if title else f"Pakistan Code {ref_number}",
                "issue_date": issue_date,
                "category": category or "Unknown",
                "document_type": default_doc_type,
            },
            headers=self._default_headers(referer=list_response.url),
            dont_filter=True,
        )

    def parse_detail(self, response):
        meta = response.meta

        # Find a PDF/document link on the detail page
        pdf_url = self._find_pdf_url(response)

        # If no PDF, store metadata only (tracked_no_pdf)
        if not pdf_url:
            logger.info(f"NEW (metadata only): PAKCODE {meta['reference_number']}")
            self._store_metadata_only(meta, document_url=None, status="tracked_no_pdf")
            return

        # Optional recency gate (you can loosen/tighten)
        cutoff = datetime.now().date() - timedelta(days=self.recency_days)
        issue_date = meta.get("issue_date")
        is_recent = (issue_date is None) or (issue_date >= cutoff)

        if not is_recent:
            logger.info(f"NEW (old, metadata only): PAKCODE {meta['reference_number']}")
            self._store_metadata_only(meta, document_url=pdf_url, status="tracked_no_pdf")
            return

        logger.info(f"NEW: PAKCODE {meta['reference_number']} ({meta.get('document_type')})")
        yield scrapy.Request(
            pdf_url,
            callback=self.save_pdf,
            errback=self.errback_log,
            meta={**meta, "document_url": pdf_url},
            headers=self._default_headers(referer=response.url),
            dont_filter=True,
        )

    # ----------------------------
    # Helpers: row extraction (robust)
    # ----------------------------
    def _extract_rows(self, response):
        """
        Tries multiple common PakistanCode patterns:
        - DataTables table#myTable
        - any table tbody rows
        - card/list layouts with "View" anchors
        """
        rows = []

        # 1) DataTables style
        tr_sel = response.css("table#myTable tbody tr")
        if not tr_sel:
            tr_sel = response.css("table tbody tr")

        for tr in tr_sel:
            tds = [self._clean_text(" ".join(td.css("::text").getall())) for td in tr.css("td")]
            link = tr.css("a::attr(href)").get()
            detail_url = response.urljoin(link) if link else None

            # Best-effort fields from row text
            title = self._extract_title(tr, tds)
            category = self._extract_category_from_cells(tds)
            ref_no = self._extract_ref_from_cells(tds)
            issue_date = self._extract_date_from_cells(tds)

            if detail_url or title:
                rows.append({
                    "title": title,
                    "detail_url": detail_url,
                    "category": category,
                    "reference_number": ref_no,
                    "issue_date": issue_date,
                    "raw_cells": tds,
                })

        if rows:
            return rows

        # 2) Fallback: anchors that look like "View"
        for a in response.css("a"):
            href = a.attrib.get("href", "")
            text = self._clean_text(" ".join(a.css("::text").getall()))
            if not href:
                continue
            # Heuristic: lots of PakistanCode detail links look like short .php endpoints
            if "View" in text or href.endswith(".php") or "/english/" in href:
                rows.append({
                    "title": text if text and text.lower() != "view" else "",
                    "detail_url": response.urljoin(href),
                    "category": "",
                    "reference_number": "",
                    "issue_date": None,
                    "raw_cells": [],
                })

        return rows

    def _find_pdf_url(self, response):
        hrefs = response.css("a::attr(href)").getall()
        hrefs = [h.strip() for h in hrefs if h and isinstance(h, str)]

        # Prefer obvious pdf links
        for h in hrefs:
            if ".pdf" in h.lower():
                return response.urljoin(h)

        # Some sites use download endpoints without .pdf in URL
        for h in hrefs:
            hl = h.lower()
            if "download" in hl or "pdf" in hl:
                return response.urljoin(h)

        return None

    # ----------------------------
    # Field extraction helpers
    # ----------------------------
    def _extract_title(self, tr, tds):
        # Prefer anchor text in the row if it exists
        a_text = self._clean_text(" ".join(tr.css("a::text").getall()))
        if a_text and a_text.lower() not in {"view", "download"}:
            return a_text
        # Otherwise best guess: longest cell
        if tds:
            return max(tds, key=len)
        return ""

    def _extract_category_from_cells(self, tds):
        # Often includes "General Laws", "Banking/Financial Laws", etc.
        for cell in tds:
            if "laws" in cell.lower():
                return cell
        return ""

    def _extract_ref_from_cells(self, tds):
        # Often looks like "XIX of 2025" or "Act No. II of 2025"
        patterns = [
            r"\b[IVXLCDM]+\s+of\s+\d{4}\b",
            r"\bAct\s+No\.?\s*[IVXLCDM]+\s+of\s+\d{4}\b",
            r"\bOrdinance\s+No\.?\s*\d+\s+of\s+\d{4}\b",
        ]
        blob = " | ".join(tds)
        for p in patterns:
            m = re.search(p, blob, flags=re.IGNORECASE)
            if m:
                return m.group(0).strip()
        return ""

    def _extract_date_from_cells(self, tds):
        # Look for something like "Promulgation Date: June 27 2025"
        blob = " ".join(tds)
        m = re.search(r"Promulgation\s*Date\s*:\s*([A-Za-z]+\s+\d{1,2}\s*,?\s*\d{4})", blob, re.IGNORECASE)
        if not m:
            m = re.search(r"(\b[A-Za-z]+\s+\d{1,2}\s*,?\s*\d{4}\b)", blob)
        if not m:
            return None
        date_str = m.group(1).strip()
        try:
            return self.parse_date(date_str)  # BaseSpider should handle
        except Exception:
            return None

    def _clean_text(self, s):
        return re.sub(r"\s+", " ", (s or "")).strip()

    def _chrono_url(self, year, action, page):
        qs = urlencode({"year": year, "action": action, "page": page})
        return f"{self.CHRONO_ENDPOINT}?{qs}"

    def _default_headers(self, referer=None):
        h = {
            "User-Agent": self.settings.get("USER_AGENT") or
                          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
        if referer:
            h["Referer"] = referer
        return h

    # ----------------------------
    # Hash + DB helpers (same style as your FBR spider)
    # ----------------------------
    def hash_urls(self, urls):
        m = hashlib.sha256()
        for url in sorted(urls):
            m.update(url.encode("utf-8"))
        return m.hexdigest()

    def get_stored_hash(self, page_url):
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            rec = PageHash.query.filter_by(page_url=page_url).first()
            return rec.content_hash if rec else None

    def store_hash(self, page_url, new_hash):
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            rec = PageHash.query.filter_by(page_url=page_url).first()
            if rec:
                rec.content_hash = new_hash
                rec.last_checked = datetime.utcnow()
            else:
                rec = PageHash(page_url=page_url, content_hash=new_hash, last_checked=datetime.utcnow())
                db.session.add(rec)
            db.session.commit()

    def _store_metadata_only(self, meta, document_url=None, status="tracked_no_pdf"):
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            regulation = Regulation(
                regulation_id=meta["regulation_id"],
                source=meta["source"],
                page_url=meta["page_url"],
                document_url=document_url,
                reference_number=meta["reference_number"],
                title=meta["title"],
                issue_date=meta.get("issue_date"),
                category=meta.get("category", "Unknown"),
                document_type=meta.get("document_type", "Law"),
                status=status,
                discovered_at=datetime.utcnow(),
            )
            db.session.add(regulation)
            db.session.commit()

    def errback_log(self, failure):
        request = failure.request
        logger.error("Request failed: %s (%s)", request.url, failure.value)
