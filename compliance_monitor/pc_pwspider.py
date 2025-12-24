import os
import re
import hashlib
from pathlib import Path
import scrapy
import logging
from datetime import datetime, timedelta
from urllib.parse import urlencode
from sqlalchemy.exc import IntegrityError

from scrapy.selector import Selector
from scrapy_playwright.page import PageMethod

from spiders import BaseSpider
from extensions import db
from models import Regulation, PageHash

logger = logging.getLogger(__name__)


class PakistanCodePlaywrightSpider(BaseSpider):
    name = "pakistan_code_monitor_pw"
    allowed_domains = ["pakistancode.gov.pk", "www.pakistancode.gov.pk"]

    HOME_URL = "https://pakistancode.gov.pk/english/index.php"
    CHRONO_ENDPOINT = "https://pakistancode.gov.pk/english/LGu0xBD.php"
    ORDINANCES_ENDPOINT = "https://pakistancode.gov.pk/english/KmTY5e.php"

    custom_settings = {
        # --- crawling hygiene ---
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 2,
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": True,
        "RETRY_TIMES": 2,
        "LOG_LEVEL": "INFO",

        # --- scrapy-playwright wiring ---
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 45_000,
    }

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
    # Playwright meta helper
    # ----------------------------
    def _pw_meta(self, *, referer=None, include_page=False):
        meta = {
            "playwright": True,
            "playwright_context": "pakcode",  # reuse cookies/session across requests
            "playwright_page_methods": [
                PageMethod("wait_for_load_state", "domcontentloaded"),
                # wait a bit for any JS / bot checks to settle
                PageMethod("wait_for_timeout", 800),
            ],
        }
        if include_page:
            meta["playwright_include_page"] = True
        return meta

    def _default_headers(self, referer=None):
        h = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
        if referer:
            h["Referer"] = referer
        return h

    # ----------------------------
    # Entry
    # ----------------------------
    def start_requests(self):
        yield scrapy.Request(
            self.HOME_URL,
            callback=self.after_home,
            headers=self._default_headers(referer="https://pakistancode.gov.pk/"),
            meta=self._pw_meta(include_page=True),
            dont_filter=False,
        )

    async def after_home(self, response):
        page = response.meta.get("playwright_page")
        try:
            logger.info("HOME status=%s url=%s", response.status, response.url)

            if response.status != 200:
                # dump for debugging
                debug_path = os.path.join(os.getcwd(), "pakcode_home_debug.html")
                with open(debug_path, "wb") as f:
                    f.write(response.body or b"")
                logger.warning("HOME not 200. Saved debug HTML to %s", debug_path)

            # 1) Chronological feeds (Playwright)
            for action in self.actions:
                url = self._chrono_url(year=self.year, action=action, page=1)
                yield scrapy.Request(
                    url,
                    callback=self.parse_chrono,
                    headers=self._default_headers(referer=self.HOME_URL),
                    meta={
                        **self._pw_meta(include_page=False),
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                            PageMethod("wait_for_selector", "div.accordion-section-title a[href], table tbody tr", state="attached", timeout=20000),
                        ],
                          "feed_kind": "chrono",
                          "year": self.year,
                          "action": action,
                          "page": 1},
                    dont_filter=False,
                )

            # 2) Ordinances feed (Playwright)
            yield scrapy.Request(
                self.ORDINANCES_ENDPOINT,
                callback=self.parse_ordinances_list,
                headers=self._default_headers(referer=self.HOME_URL),
                meta={**self._pw_meta(include_page=False), "feed_kind": "ordinances"},
                dont_filter=False,
            )
        finally:
            if page:
                await page.close()

    # ----------------------------
    # Chronological list
    # ----------------------------
    async def parse_chrono(self, response):
        logger.info("URL=%s status=%s", response.url, response.status)
        logger.info("has myTable? %s", bool(response.css("table#myTable")))
        logger.info("tbody tr count=%d", len(response.css("table tbody tr")))
        logger.info("first500=%r", response.text[:500])
        year = response.meta["year"]
        action = response.meta["action"]
        page_num = response.meta["page"]

        rows = self._extract_rows(response)
        if not rows:
            logger.info("PakistanCode chrono: no rows (year=%s action=%s page=%s status=%s)",
                        year, action, page_num, response.status)
            return

        # Hash on page 1
        if page_num == 1:
            stable_ids = [r.get("detail_url") or r.get("title") or "" for r in rows]
            current_hash = self.hash_urls([s for s in stable_ids if s])

            hash_key = f"PAKCODE:CHRONO:{year}:{action}:PAGE1"
            stored_hash = self.get_stored_hash(hash_key)
            if (not self.test_mode) and stored_hash and stored_hash == current_hash:
                logger.info("PakistanCode chrono unchanged (year=%s action=%s) - skipping", year, action)
                return
            self.store_hash(hash_key, current_hash)

        for r in rows:
            async for req in self._handle_list_row_pw(r, list_response=response, default_doc_type="Law"):
                yield req

        # pagination
        if page_num < self.max_pages:
            next_page = page_num + 1
            next_url = self._chrono_url(year=year, action=action, page=next_page)
            yield scrapy.Request(
                next_url,
                callback=self.parse_chrono,
                headers=self._default_headers(referer=response.url),
                meta={**response.meta, "page": next_page, **self._pw_meta(include_page=False)},
                dont_filter=False,
            )

    # ----------------------------
    # Ordinances list
    # ----------------------------
    async def parse_ordinances_list(self, response):
        logger.info("URL=%s status=%s", response.url, response.status)
        logger.info("has myTable? %s", bool(response.css("table#myTable")))
        logger.info("tbody tr count=%d", len(response.css("table tbody tr")))
        logger.info("first500=%r", response.text[:500])
        with open("/tmp/pakcode_debug.html", "wb") as f:
            f.write(response.body)
        rows = self._extract_rows(response)
        if not rows:
            logger.warning("PakistanCode ordinances: no rows (status=%s).", response.status)
            return

        stable_ids = [r.get("detail_url") or r.get("title") or "" for r in rows]
        current_hash = self.hash_urls([s for s in stable_ids if s])

        hash_key = "PAKCODE:ORDINANCES:PAGE1"
        stored_hash = self.get_stored_hash(hash_key)
        if (not self.test_mode) and stored_hash and stored_hash == current_hash:
            logger.info("PakistanCode ordinances unchanged - skipping")
            return
        self.store_hash(hash_key, current_hash)

        for r in rows:
            async for req in self._handle_list_row_pw(r, list_response=response, default_doc_type="Ordinance"):
                yield req

    # ----------------------------
    # Follow detail page with Playwright, then download PDF with Scrapy
    # ----------------------------

    def save_pdf(self, response):
        meta = response.meta
        regulation_id = meta["regulation_id"]
        pdf_url = meta.get("document_url") or response.url

        # --- Save file ---
        out_dir = Path(getattr(self, "pdf_dir", None) or "./downloaded_pdfs/pakistancode")
        out_dir.mkdir(parents=True, exist_ok=True)

        file_path = out_dir / f"{regulation_id}.pdf"
        file_path.write_bytes(response.body)

        # --- Hash content ---
        content_hash = hashlib.sha256(response.body).hexdigest()

        # --- DB write / update ---
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")

        with self.flask_app.app_context():
            # If you already inserted a placeholder row earlier, update it
            reg = Regulation.query.filter_by(regulation_id=regulation_id).first()
            if not reg:
                # fallback: create if missing
                reg = Regulation(
                    regulation_id=regulation_id,
                    source=meta.get("source", "PAKCODE"),
                    page_url=meta.get("page_url"),
                    reference_number=meta.get("reference_number"),
                    title=meta.get("title"),
                    issue_date=meta.get("issue_date"),
                    category=meta.get("category", "Unknown"),
                    document_type=meta.get("document_type", "Law"),
                    discovered_at=datetime.utcnow(),
                )
                db.session.add(reg)

            reg.document_url = pdf_url
            reg.file_path = str(file_path)
            reg.content_hash = content_hash
            reg.status = "downloaded"
            reg.last_checked = datetime.utcnow()

            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                logger.info("Duplicate regulation_id on save_pdf (already stored): %s", regulation_id)


    async def _handle_list_row_pw(self, r, list_response, default_doc_type="Law"):
        title = (r.get("title") or "").strip()
        detail_url = r.get("detail_url")
        category = (r.get("category") or "").strip()
        ref_number = (r.get("reference_number") or "").strip()
        issue_date = r.get("issue_date")

        # if not ref_number:
        #     logger.warning("EMPTY REF for title=%r url=%s blob=%r", title[:80], detail_url, r.get("raw_cells"))


        if not detail_url:
            return

        if not ref_number:
            ref_number = self.generate_fallback_ref(detail_url)
            logger.info(f"Generated fallback ref: {ref_number} for URL: {detail_url[-20:]}")

        regulation_id = self.generate_regulation_id("PAKCODE", ref_number, issue_date)
        
        # TEMPORARILY COMMENTING OUT FOR TESTING
        # if self.regulation_exists(regulation_id):
        #     logger.info(f"SKIPPING Existing: {ref_number} (ID: {regulation_id})")
        #     return

        yield scrapy.Request(
            detail_url,
            callback=self.parse_detail_pw,
            headers=self._default_headers(referer=list_response.url),
            meta={
                **self._pw_meta(include_page=True),
                # INJECT THE CLICKS HERE
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "domcontentloaded"),
                    # 1. Click the "Print/Download" tab (matches your colleague's logic)
                    PageMethod("click", "a:has-text('PDF')", timeout=10000), 
                    # 2. Wait for the PDF link to appear in the DOM
                    PageMethod("wait_for_timeout", 3000),
                    #PageMethod("wait_for_selector", "a[href*='.pdf']", state="attached", timeout=8000),
                ],
                "regulation_id": regulation_id,
                "source": "PAKCODE",
                "page_url": detail_url,
                "reference_number": ref_number,
                "title": title[:200] if title else f"Pakistan Code {ref_number}",
                "issue_date": issue_date,
                "category": category or "Unknown",
                "document_type": default_doc_type,
            },
            dont_filter=False,
        )

    def _strip_pw(self, meta: dict) -> dict:
        return {k: v for k, v in (meta or {}).items() if not k.startswith("playwright")}

    async def parse_detail_pw(self, response):
        page = response.meta.get("playwright_page")
        meta = response.meta
        try:
            if response.status != 200:
                logger.warning("Detail page not 200 (%s): %s", response.status, response.url)
                self._store_metadata_only(meta, document_url=None, status="tracked_no_pdf")
                return

            # Find PDF link
            pdf_url = response.css("div#tab4 a[href*='.pdf']::attr(href)").get()

            if not pdf_url:
                pdf_url = self._find_pdf_url(response)

            if not pdf_url:
                logger.info("NEW (metadata only): PAKCODE %s (no pdf link)", meta["reference_number"])
                self._store_metadata_only(meta, document_url=None, status="tracked_no_pdf")
                return
            
            pdf_url = response.urljoin(pdf_url)

            # Recency gate (optional)
            cutoff = datetime.now().date() - timedelta(days=self.recency_days)
            issue_date = meta.get("issue_date")
            is_recent = (issue_date is None) or (issue_date >= cutoff)
            if not is_recent:
                logger.info("NEW (old, metadata only): PAKCODE %s", meta["reference_number"])
                self._store_metadata_only(meta, document_url=pdf_url, status="tracked_no_pdf")
                return

            # IMPORTANT: copy cookies from Playwright context into Scrapy PDF request
            cookies_dict = {}
            if page:
                try:
                    cookies = await page.context.cookies()
                    cookies_dict = {c["name"]: c["value"] for c in cookies} # if c.get("name")}
                except Exception:
                    cookies_dict = {}

            logger.info("\nNEW: PAKCODE %s (%s)", meta["reference_number"], meta.get("document_type"))
            logger.info(f"DOWNLOADING PDF: {pdf_url}")
            yield scrapy.Request(
                pdf_url,
                callback=self.save_pdf,  # BaseSpider should save + write DB
                errback=self.errback_log,
                headers=self._default_headers(referer=response.url),
                cookies=cookies_dict, # or None,
                meta={**self._strip_pw(meta), "document_url": pdf_url},
                dont_filter=False,
            )
        finally:
            if page:
                await page.close()

    # ----------------------------
    # Row extraction (same as your original)
    # ----------------------------
    def _extract_rows(self, response):
            rows = []

            sections = response.css("div.accordion-section")
            logger.info("Accordion sections found: %d (status=%s url=%s)",
                        len(sections), response.status, response.url)

            for sec in sections:
                a = sec.css("div.accordion-section-title a[href]")
                href = sec.css("div.accordion-section-title a::attr(href)").get()
                # href = a.attrib.get("href") if a else None
                if not href:
                    continue

                # Title text includes "1." etc – keep it, or strip it if you want
                title = self._clean_text(" ".join(a.css("::text").getall()))
                title = self._clean_text(" ".join(sec.css("div.accordion-section-title a::text").getall()))


                detail_url = response.urljoin(href.strip())
                logger.info("Detail href=%r => %s", href, detail_url)


                content = sec.css("div.accordion-section-content")
                blob = self._clean_text(" ".join(content.css("::text").getall()))

                # Example blob: "General Laws | VII of 2025 | Promulgation Date: July 08 2025."
                category = ""
                if "|" in blob:
                    category = self._clean_text(blob.split("|", 1)[0])

                ref_no = self._extract_ref_from_cells([blob])  # reuse your existing regex helper
                issue_date = None

                m = re.search(r"Promulgation\s*Date\s*:\s*([A-Za-z]+\s+\d{1,2}\s+\d{4})", blob, re.IGNORECASE)
                if m:
                    try:
                        issue_date = self.parse_date(m.group(1).strip())
                    except Exception:
                        issue_date = None

                rows.append({
                    "title": title,
                    "detail_url": detail_url,
                    "category": category,
                    "reference_number": ref_no,
                    "issue_date": issue_date,
                    "raw_cells": [blob],
                })

            return rows

    def _find_pdf_url(self, response):
        # 1) normal hrefs
        for h in response.css("a::attr(href)").getall():
            if h and ".pdf" in h.lower():
                return response.urljoin(h.strip())

        # 2) onclick patterns (window.open('...pdf'))
        onclicks = response.css("[onclick]::attr(onclick)").getall()
        for oc in onclicks:
            if not oc:
                continue
            m = re.search(r"['\"]([^'\"]+\.pdf[^'\"]*)['\"]", oc, re.IGNORECASE)
            if m:
                return response.urljoin(m.group(1).strip())

        # 3) raw HTML fallback
        m = re.search(r"(https?://[^\s\"']+\.pdf[^\s\"']*)", response.text, re.IGNORECASE)
        if m:
            return m.group(1)

        return None


    # def _find_pdf_url(self, response):
    #     hrefs = response.css("a::attr(href)").getall()
    #     hrefs = [h.strip() for h in hrefs if h and isinstance(h, str)]

    #     for h in hrefs:
    #         if ".pdf" in h.lower():
    #             return response.urljoin(h)

    #     for h in hrefs:
    #         hl = h.lower()
    #         if "download" in hl or "pdf" in hl or "pdffiles" in hl:
    #             return response.urljoin(h)

    #     return None

    # ---- field helpers ----
    def _extract_title(self, tr, tds):
        a_text = self._clean_text(" ".join(tr.css("a::text").getall()))
        if a_text and a_text.lower() not in {"view", "download"}:
            return a_text
        return max(tds, key=len) if tds else ""

    def _extract_category_from_cells(self, tds):
        for cell in tds:
            if "laws" in cell.lower():
                return cell
        return ""

    def _extract_ref_from_cells(self, tds):
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
        blob = " ".join(tds)
        m = re.search(r"Promulgation\s*Date\s*:\s*([A-Za-z]+\s+\d{1,2}\s*,?\s*\d{4})", blob, re.IGNORECASE)
        if not m:
            m = re.search(r"(\b[A-Za-z]+\s+\d{1,2}\s*,?\s*\d{4}\b)", blob)
        if not m:
            return None
        try:
            return self.parse_date(m.group(1).strip())
        except Exception:
            return None

    def _clean_text(self, s):
        return re.sub(r"\s+", " ", (s or "")).strip()

    def _chrono_url(self, year, action, page):
        qs = urlencode({"year": year, "action": action, "page": page})
        return f"{self.CHRONO_ENDPOINT}?{qs}"

    # ----------------------------
    # Hash + DB helpers (same as before)
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
