from urllib.parse import urlparse, parse_qs, quote_plus
import scrapy
import hashlib
import re
from datetime import datetime, timedelta
import os
import json
import logging

from extensions import db
from models import Regulation, PageHash

logger = logging.getLogger(__name__)




class BaseSpider(scrapy.Spider):
    """Base spider with common functionality"""

    def __init__(self, flask_app=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flask_app = flask_app
    
    """generates unique id - always same output for same input"""
    def generate_regulation_id(self, source, ref_number, date=None):
        if date:
            identifier = f"{source}_{ref_number}_{date}"
        else:
            identifier = f"{source}_{ref_number}_NODATE"
        return hashlib.md5(identifier.encode()).hexdigest()
    
    """Where is it getting the date from?"""
    def parse_date(self, date_str):
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        m = re.match(r'/Date\((\d+)\)/', date_str)
        if m:
            try:
                ms = int(m.group(1))
                return datetime.fromtimestamp(ms / 1000.0).date()
            except Exception:
                pass

        date_formats = [
            '%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d.%m.%Y',
            '%B %d, %Y', '%B %d %Y', '%d %B %Y', '%d-%b-%Y',
            '%d %b %Y',  '%b %d %Y', '%d/%b/%Y', "%b %d, %Y"
        ]
        
        """Couldn't this end up trying a wrong type that fits?
        How does it differentiate bw d, m and y?"""
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date string: {date_str!r}")
        return None
    
    def regulation_exists(self, regulation_id):
        """Check if regulation exists in database"""
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            return (
                Regulation.query
                .filter_by(regulation_id=regulation_id)
                .first() 
                is not None
            )
    
    def extract_ref_from_url_or_text(self, url, text):
        """Extract reference number from URL or text"""
        combined = f"{text} {url}"

        patterns = [
            r'(S\.?R\.?O\.?\s*\d+\s*\([IVX]+\)\s*/\s*\d{4})',
            r'(SRO\s*\d+)',
            r'(Circular\s*No\.?\s*\d+)',
            r'(Ordinance\s*No\.?\s*\d+)',
            r'(Notification\s*No\.?\s*\d+)',
            r'(\d{4,})',
        ]

        for pattern in patterns:
            match = re.search(pattern, combined, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    def generate_fallback_ref(self, url):
        """
        Generate a stable-ish fallback reference from:
        - wpdmdl id if present
        - filename-like tail otherwise
        """
        try:
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)

            if "wpdmdl" in qs and qs["wpdmdl"]:
                return f"SECP-WPDM-{qs['wpdmdl'][0]}"

            # fallback to path segment
            filename = parsed.path.rstrip("/").split("/")[-1] or "document"
            filename = filename.replace(".pdf", "").replace(".PDF", "")

            # shorten
            return f"SECP-{filename[:50]}"

        except Exception:
            # absolute last-resort fallback
            safe = re.sub(r"[^A-Za-z0-9]+", "-", url)[:50]
            return f"SECP-{safe}"


class FBRSpider(BaseSpider):
    name = 'fbr_monitor'
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 2,
        'ROBOTSTXT_OBEY': True,
    }

    def __init__(self, flask_app=None, test_mode=False, *args, **kwargs):
        super().__init__(flask_app=flask_app, *args, **kwargs)
        self.test_mode = test_mode 

    LOAD_URL = "https://www.fbr.gov.pk/Home/LoadSROs"
    api_url = "https://www.fbr.gov.pk/Home/LoadSROs"
    
    """These methods are not things that we need to separately call, 
    Scrapy's engine automatically checks if these methods exist and calls them
    when we run runner.crawl(FBRSpider)
    it checks for a start_requests method, start_requests specifies 
    callback as self.parse method.
    Parse - if it comes acorss a link, it calls self.process_item on the link
    Process_item calls parse_date (if there is one it'll parse it)
    and generate_regulation_id
    Checks if regulation_id exists in database, and if it doesn't 
    it calls save_pdf and adds it and saves it to db"""
    def start_requests(self):

        """Different site links defined
        Need to check if sites need to be modified - i.e. circulars and stuff included?
        What parts of the FBR website do we need to monitor?
        """

        self.targets = [
                {"department": "Income Tax", "category": "Income Tax", "doc_type": "SRO"},
                {"department": "Sales Tax",  "category": "Sales Tax",  "doc_type": "SRO"},
                {"department": "Customs",    "category": "Customs",    "doc_type": "SRO"},

        ]
        
        """Initially downloads this page"""
        for t in self.targets:
            show_url = (
                "https://www.fbr.gov.pk/ShowSROs?Department="
                + quote_plus(t["department"])
            )


            referer = f"https://www.fbr.gov.pk/ShowSROs?Department={t['department'].replace(' ', '+')}"


            yield scrapy.Request(
                show_url, 
                callback=self.post_loadsros,
                meta={"target": t, "cookiejar": t["department"]},
                #method="POST",
                dont_filter=True,
                headers={
                    "User-Agent": self.settings.get("USER_AGENT"),
                    "Accept": "text/html,application/xhtml+xml",
                },
            )

    def post_loadsros(self, response):
        t = response.meta["target"]

        base_form = {
            "draw": "1",
            "start": "0",
            "length": "100",
            "search[value]": "",
            "search[regex]": "false",
            "order[0][column]": "0",
            "order[0][dir]": "asc",
            "columns[0][data]": "SRONumber",
            "columns[0][name]": "SRONumber",
            "columns[0][searchable]": "true",
            "columns[0][orderable]": "true",
            "columns[0][search][value]": "",
            "columns[0][search][regex]": "false",
            "columns[1][data]": "Title",
            "columns[1][name]": "Title",
            "columns[1][searchable]": "true",
            "columns[1][orderable]": "true",
            "columns[1][search][value]": "",
            "columns[1][search][regex]": "false",
            "columns[2][data]": "CreationDate",
            "columns[2][name]": "CreationDate",
            "columns[2][searchable]": "true",
            "columns[2][orderable]": "true",
            "columns[2][search][value]": "",
            "columns[2][search][regex]": "false",
            "columns[3][data]": "CategoryTitle",
            "columns[3][name]": "CategoryTitle",
            "columns[3][searchable]": "true",
            "columns[3][orderable]": "true",
            "columns[3][search][value]": "",
            "columns[3][search][regex]": "false",
            "columns[4][data]": "UploadedFile1",
            "columns[4][name]": "",
            "columns[4][searchable]": "true",
            "columns[4][orderable]": "true",
            "columns[4][search][value]": "",
            "columns[4][search][regex]": "false",
        }

        form = base_form.copy()
        form["department"] = t["department"]
        # IMPORTANT: don't send form["source"] unless you later confirm it's needed.
        # Browser doesn't send it; session carries it.

        yield scrapy.FormRequest(
            url=self.api_url,
            formdata=form,
            callback=self.parse_api,
            meta={"target": t, "cookiejar": response.meta["cookiejar"]},
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://www.fbr.gov.pk",
                "Referer": response.url,
                "User-Agent": self.settings.get("USER_AGENT"),
            },
            dont_filter=True
        )

    def parse_api(self, response):
        target = response.meta["target"]
        logger.info(f"Parsing API for {target['department']} ({response.status})")


        raw = response.text.strip()

        # 1) Decode JSON safely
        try:
            payload = response.json()
        except Exception:
            try:
                payload = json.loads(raw)
            except Exception:
                logger.error("FBR LoadSROs not valid JSON. First 300 chars: %r", raw[:300])
                return

        # 2) Handle double-encoded JSON (payload is a string)
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                logger.error("FBR returned JSON string (not an object). Value: %r", payload[:300])
                logger.error("Raw response first 300 chars: %r", raw[:300])
                return

        # 3) Sanity check
        if not isinstance(payload, dict):
            logger.error("Unexpected payload type: %s. Raw first 300 chars: %r",
                        type(payload), raw[:300])
            return

        # 4) Extract rows (DataTables style)
        rows = payload.get("data") or payload.get("Data") or []
        if not rows:
            logger.warning(f"No rows in API response for {target['department']}")
            logger.info(f"Payload keys: {list(payload.keys())}")
            logger.info(f"Raw first 300 chars: %r", raw[:300])
            return

        logger.info(f"First row keys: {list(rows[0].keys())}")
        logger.info(f"First row sample: {rows[0]}")

        
        stable_ids = []
        for r in rows:
            stable_ids.append(
                str(
                    r.get("SRONumber")
                    or r.get("SRO")
                    or r.get("SRONo")
                    or r.get("Title")
                    or r.get("UploadedFile1")
                    or ""
                )
            )
        current_hash = self.hash_urls(sorted([s for s in stable_ids if s]))

        stored_hash = self.get_stored_hash(f"FBR:{target['department']}")
        if stored_hash and stored_hash == current_hash:
            logger.info(f"API unchanged for {target['department']} - skipping")
            return

        six_months_ago = datetime.now().date() - timedelta(days=100)

        for r in rows:
            ref_number = str(r.get("SRONumber") or r.get("SRO") or r.get("SRONo") or "").strip()
            title_text = (r.get("Title") or r.get("title") or "").strip()
            date_str = (r.get("CreationDate") or r.get("creationDate") or "").strip()

            file_field = (
                r.get("UploadedFile1")
                or r.get("UploadedFile")
                or r.get("FilePath")
                or r.get("filePath")
                or ""
            )

            if not ref_number:
                ref_number = self.generate_fallback_ref(file_field or title_text)

            issue_date = self.parse_date(date_str) if date_str else None
            regulation_id = self.generate_regulation_id("FBR", ref_number, issue_date)

            if self.regulation_exists(regulation_id):
                continue

            if isinstance(file_field, str) and file_field.startswith("http"):
                pdf_url = file_field
            else:
                pdf_url = f"https://download1.fbr.gov.pk/Docs/{file_field}".rstrip("/")

            is_recent = issue_date is None or issue_date >= six_months_ago
            title = title_text[:200] if title_text else f"FBR {ref_number}"

            if is_recent and pdf_url:
                logger.info(f"NEW: FBR {ref_number} ({target['department']})")
                yield scrapy.Request(
                    pdf_url,
                    callback=self.save_pdf,
                    meta={
                        "regulation_id": regulation_id,
                        "source": "FBR",
                        "page_url": f"https://www.fbr.gov.pk/ShowSROs?Department={target['department'].replace(' ', '+')}",
                        "document_url": pdf_url,
                        "reference_number": ref_number,
                        "title": title,
                        "issue_date": issue_date,
                        "category": target["category"],
                        "document_type": target["doc_type"],
                    },
                )
            else:
                logger.info(f"NEW (metadata only): FBR {ref_number}")
                if not self.flask_app:
                    raise RuntimeError("flask_app not set on spider")
                with self.flask_app.app_context():
                    regulation = Regulation(
                        regulation_id=regulation_id,
                        source="FBR",
                        page_url=f"https://www.fbr.gov.pk/ShowSROs?Department={target['department'].replace(' ', '+')}",
                        document_url=pdf_url,
                        reference_number=ref_number,
                        title=title,
                        issue_date=issue_date,
                        category=target["category"],
                        document_type=target["doc_type"],
                        status="tracked_no_pdf",
                        discovered_at=datetime.utcnow(),
                    )
                    db.session.add(regulation)
                    db.session.commit()

        self.store_hash(f"FBR:{target['department']}", current_hash)

    def hash_urls(self, urls):
        """Generate hash from sorted list of URLs"""
        import hashlib
        m = hashlib.sha256()
        for url in sorted(urls):
            m.update(url.encode('utf-8'))
        return m.hexdigest()
    
    def get_stored_hash(self, page_url):
        """Get stored hash for this page from database"""
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            # Check if we have a PageHash table/model
            hash_record = PageHash.query.filter_by(page_url=page_url).first()
            return hash_record.content_hash if hash_record else None
    
    def store_hash(self, page_url, new_hash):
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            """Store new hash for this page"""
            hash_record = PageHash.query.filter_by(page_url=page_url).first()
            if hash_record:
                hash_record.content_hash = new_hash
                hash_record.last_checked = datetime.utcnow()
            else:
                hash_record = PageHash(
                    page_url=page_url,
                    content_hash=new_hash,
                    last_checked=datetime.utcnow()
                )
                db.session.add(hash_record)
            db.session.commit()
    
    def extract_date_from_text(self, text):
        """Extract date from text"""
        date_patterns = [
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[\s,]+(\d{4})',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        return None

    def save_pdf(self, response):
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            meta = response.meta
            pdf_content = response.body
            """Hashing the pdf, unique fingerprint, so that
            if FBR updates the PDF - the hash changes
            Can detect if same regulation but content changed"""
            content_hash = hashlib.sha256(pdf_content).hexdigest()
            
            safe_ref = re.sub(r'[^\w\-]', '_', meta['reference_number'])
            filename = f"{meta['source']}_{safe_ref}_{meta['issue_date']}.pdf"
            filepath = os.path.join('./downloads/pdfs', filename)
            
            with open(filepath, 'wb') as f:
                f.write(pdf_content)
            
            # Save to database
            regulation = Regulation(
                regulation_id=meta['regulation_id'],
                source=meta['source'],
                page_url=meta['page_url'],
                document_url=meta['document_url'],
                reference_number=meta['reference_number'],
                title=meta['title'],
                issue_date=meta['issue_date'],
                category=meta['category'],
                document_type=meta['document_type'],
                content_hash=content_hash,
                file_path=filepath,
                status='new',
                discovered_at=datetime.utcnow()
            )
            
            
            db.session.add(regulation)
            db.session.commit()
            
            logger.info(f"Saved: {meta['source']} {meta['reference_number']}")




class SECPSpider(BaseSpider):
    name = 'secp_monitor'
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 2,
        'ROBOTSTXT_OBEY': True,
    }
    
    def __init__(self, flask_app=None, test_mode=False, limit=5, recency_days=180, *args, **kwargs):
        super().__init__(flask_app=flask_app, *args, **kwargs)
        self.test_mode = test_mode
        self.limit = int(limit) if limit else 5  # For testing, limit number of documents processed
        self.recency_days = int(recency_days)
        self.cutoff_date = datetime.now().date() - timedelta(days=self.recency_days)

    def extract_date_from_text(self, text):
        """Extract date from text"""
        date_patterns = [
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[\s,]+(\d{4})',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        return None
    
    start_urls = [
        'https://www.secp.gov.pk/laws/notifications/',
        'https://www.secp.gov.pk/laws/ordinances/',
    ]
    
    def parse(self, response):
        logger.info("Test Mode: %s", self.test_mode)

        if 'notifications' in response.url:
            doc_type = 'Notification'
        elif 'ordinances' in response.url:
            doc_type = 'Ordinance'
        else:
            doc_type = 'Document'

        #After . we specify the class of the tr - table row
        rows = response.css("tr.download-row-table")
        logger.info("SECP: %s rows found on %s", len(rows), response.url)

        if not rows:
            logger.warning(f"SECP: No download rows found on {response.url}")
        
        if self.test_mode:
            """FOR NOW ENFORCING LIMIT ON SECP ROWS SCRAPING TO 5"""
            logger.info(f"SECP: Test mode active - in SECPSpider Stage 2, with limit {self.limit}")
            #rows = rows[:self.limit]
            #logger.info("SECP: Test mode active - limiting to %d rows", self.limit)

        for row in rows:
            #As in we're saying, for the row's css, select the td with class date download-date
            #What is the :: doing? It selects the text node within that element
            date_str = (row.css("td.download-date::text").get() or "").strip()
            # Why don't we need to include sorting_1 after download_title?
            title = (row.css("td.download-title::text").get() or "").strip()
            href = row.css("td.download-link a::attr(href)").get()

            if not href:
                logger.warning(f"SECP: No document link found in row on {response.url}")
                continue
            
            issue_date = self.parse_date(date_str) if date_str else None

            if issue_date and issue_date < self.cutoff_date:
                #logger.info(f"SECP: Skipping old document dated {issue_date} - before cutoff {self.cutoff_date}")
                continue   


            if "wpdmdl" in href:
                #print("\n\nFOUND WPDMDL LINK - branch 1\n")
                pdf_url = response.urljoin(href)

                ref_number = self.generate_fallback_ref(pdf_url)
                
                regulation_id = self.generate_regulation_id("SECP", ref_number) #, issue_date) 
                

                if self.regulation_exists(regulation_id):
                    #logger.info("Regulation already exists, skipping...")
                    #logger.info(f"Generated regulation id: {regulation_id}")
                    #logger.info(f"Generated ref number: {ref_number}\n")
                    continue

                logger.info(f"NEW: SECP direct download {ref_number}")
                logger.info(f"Generated ref number: {ref_number}")
                logger.info(f"regulation_id: {regulation_id}")
                logger.info(f"URL: {pdf_url}\n")

                yield scrapy.Request(
                    pdf_url,
                    callback=self.save_pdf,
                    meta={
                        "regulation_id": regulation_id,
                        "source": "SECP",
                        "page_url": response.url,
                        "document_url": pdf_url,
                        "reference_number": ref_number,
                        "title": title or ref_number,
                        "issue_date": issue_date,
                        "category": "Corporate/Securities",
                        "document_type": doc_type,
                    },
                    dont_filter=True,
                )
            
            else:
                yield scrapy.Request(
                    response.urljoin(href),
                    callback=self.parse_document_page,
                    meta={
                        "listing_url":response.url,
                        "doc_type": doc_type,
                        "title_from_list": title,
                        "issue_date_from_list": issue_date
                    },
                    dont_filter=True,
                )

    def parse_document_page(self, response):
        doc_type = response.meta.get("doc_type", "Document")
        listing_url = response.meta.get("listing_url", response.url)
        title = response.meta.get("title_from_list") or ""
        issue_date = response.meta.get("issue_date_from_list")

        page_title = (response.css("h1::text").get() or "").strip()
        if page_title:
            title = page_title

        # Try to find a direct PDF link
        pdf_href = response.css('a[href*=".pdf"]::attr(href)').get()

        # Otherwise find the wpdmdl link here
        if not pdf_href:
            pdf_href = response.css('a[href*="wpdmdl="]::attr(href)').get()

        if not pdf_href:
            logger.warning(f"SECP: No download link found on {response.url}")
            return

        pdf_url = response.urljoin(pdf_href)

        full_text = " ".join(
            t.strip() for t in response.css("body *::text").getall() if t.strip()
        )

        ref_number = self.extract_ref_from_url_or_text(pdf_url, full_text)
        if not ref_number:
            ref_number = self.generate_fallback_ref(pdf_url)

        regulation_id = self.generate_regulation_id("SECP", ref_number) #, issue_date)

        if self.regulation_exists(regulation_id):
            return

        logger.info(f"NEW: SECP {ref_number}")

        yield scrapy.Request(
            pdf_url,
            callback=self.save_pdf,
            meta={
                "regulation_id": regulation_id,
                "source": "SECP",
                "page_url": listing_url,
                "document_url": pdf_url,
                "reference_number": ref_number,
                "title": title or ref_number,
                "issue_date": issue_date,
                "category": "Corporate/Securities",
                "document_type": doc_type,
            },
            dont_filter=True,
        )

    def save_pdf(self, response):
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            meta = response.meta
            content = response.body
            content_hash = hashlib.sha256(content).hexdigest()
            
            safe_ref = re.sub(r'[^\w\-]', '_', meta['reference_number'])
            filename = f"{meta['source']}_{safe_ref}.pdf"
            filepath = os.path.join('./downloads/pdfs', filename)
            
            with open(filepath, 'wb') as f:
                f.write(content)
            
            regulation = Regulation(
                regulation_id=meta['regulation_id'],
                source=meta['source'],
                page_url=meta['page_url'],
                document_url=meta['document_url'],
                reference_number=meta['reference_number'],
                title=meta['title'],
                issue_date=meta['issue_date'],
                category=meta['category'],
                document_type=meta['document_type'],
                content_hash=content_hash,
                file_path=filepath,
                status='new',
                discovered_at=datetime.utcnow()
            )
            
            db.session.add(regulation)
            db.session.commit()
            #print(f"Regulation {meta['reference_number']} added to db")


class PCPGazetteSpider(BaseSpider):
    """
    Spider for The Gazette of Pakistan (PCP Download page)

    URL: http://www.pcp.gov.pk/Download

    We only keep:
      - rows where hidden Parts column == 2  (Part-II)
      - and we treat them as Statutory Notifications / SROs
    """

    name = "pcp_gazette_monitor"
    allowed_domains = ["pcp.gov.pk", "www.pcp.gov.pk"]
    start_urls = ["http://www.pcp.gov.pk/Download"]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 2,
        "ROBOTSTXT_OBEY": True,
    }

    def __init__(self, flask_app = None, test_mode=False, *args, **kwargs):
        super().__init__(flask_app=flask_app, *args, **kwargs)
        self.test_mode = test_mode
        self.test_mode = test_mode
        # only keep reasonably recent entries to avoid flooding
        self.six_months_ago = datetime.now().date() - timedelta(days=180)

    def parse(self, response):
        logger.info("PCP Gazette: parsing %s", response.url)

        # This matches your snippet exactly:
        # <table id="myTable"><tbody><tr>...</tr></tbody></table>
        rows = response.css("table#myTable tbody tr")

        if not rows:
            logger.warning("PCP Gazette: No rows found in table#myTable on %s", response.url)

        seen = 0
        yielded = 0

        for row in rows:
            seen += 1

            job_id = (row.css("td:nth-child(1)::text").get() or "").strip()
            department = (row.css("td:nth-child(2)::text").get() or "").strip()
            title = (row.css("td:nth-child(3)::text").get() or "").strip()
            date_str = (row.css("td:nth-child(4)::text").get() or "").strip()
            pdf_href = row.css("td:nth-child(5) a::attr(href)").get()
            parts_text = (row.css("td:nth-child(7)::text").get() or "").strip()  # hidden "Parts" column

            # We only want Part-II (client requirement)
            # In your snippet, Parts = 2 for Part-II, 3 for Part-III.
            if parts_text != "2":
                continue

            if not pdf_href:
                logger.warning("PCP Gazette: No PDF href for job_id=%s on %s", job_id, response.url)
                continue

            pdf_url = response.urljoin(pdf_href)

            # Compose a row_text string to feed into regex-based helpers
            row_text = f"{job_id} {department} {title} {date_str}"

            # Date from the Date column: examples "May 11, 2024", "December 31, 2022"
            # Your BaseSpider.parse_date already supports '%B %d, %Y'
            issue_date = self.parse_date(date_str) if date_str else None

            # Optional: only keep fairly recent entries to avoid backfilling years at once
            if issue_date and issue_date < self.six_months_ago:
                continue

            # Reference number:
            # try to extract an S.R.O-like ref; else fall back to Job ID; else URL-based fallback
            #ref_number = self.extract_ref_from_url_or_text(pdf_url, row_text)
            ref_number = (job_id or "").strip()
            if not ref_number:
                    ref_number = self.generate_fallback_ref(pdf_url)

            regulation_id = self.generate_regulation_id("PCP", ref_number, issue_date)
            if self.regulation_exists(regulation_id):
                continue

            display_title = title or job_id or f"PCP Gazette {ref_number}"
            logger.info(
                "PCP Gazette NEW: ref=%s, job_id=%s, date=%s, parts=%s, url=%s",
                ref_number, job_id, issue_date, parts_text, pdf_url
            )

            meta = {
                "regulation_id": regulation_id,
                "source": "PCP",
                "page_url": response.url,
                "document_url": pdf_url,
                "reference_number": ref_number,
                "title": display_title[:200],
                "issue_date": issue_date,
                "category": "Gazette Part-II",
                "document_type": "SRO",   # per client: Part-II statutory notifications / SROs
            }

            yielded += 1
            yield scrapy.Request(
                pdf_url,
                callback=self.save_pdf,
                meta=meta,
                dont_filter=True,
            )

            # In test mode, stop early so you can sanity-check without hammering
            if self.test_mode and yielded >= 5:
                logger.info("PCP Gazette: test_mode=True, stopping after %d docs", yielded)
                break

        logger.info(
            "PCP Gazette: scanned %d rows, queued %d Part-II SROs",
            seen, yielded
        )

    def save_pdf(self, response):
        """
        Same idea as FBR/SECP save_pdf, adapted for PCP.
        """
        if not self.flask_app:
            raise RuntimeError("flask_app not set on spider")
        with self.flask_app.app_context():
            meta = response.meta
            pdf_content = response.body
            content_hash = hashlib.sha256(pdf_content).hexdigest()

            safe_ref = re.sub(r"[^\w\-]", "_", meta["reference_number"])
            date_str = meta["issue_date"].isoformat() if meta["issue_date"] else "NODATE"
            filename = f"{meta['source']}_{safe_ref}_{date_str}.pdf"
            filepath = os.path.join("./downloads/pdfs", filename)

            with open(filepath, "wb") as f:
                f.write(pdf_content)

            regulation = Regulation(
                regulation_id=meta["regulation_id"],
                source=meta["source"],
                page_url=meta["page_url"],
                document_url=meta["document_url"],
                reference_number=meta["reference_number"],
                title=meta["title"],
                issue_date=meta["issue_date"],
                category=meta["category"],
                document_type=meta["document_type"],
                content_hash=content_hash,
                file_path=filepath,
                status="new",
                discovered_at=datetime.utcnow(),
            )

            db.session.add(regulation)
            db.session.commit()

            logger.info("PCP Gazette: Saved %s %s", meta["source"], meta["reference_number"])





