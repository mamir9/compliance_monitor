"""
Self-Contained Compliance Monitoring System
Complete Flask application with built-in scraping and scheduling

Features:
- SQLite database (no PostgreSQL needed)
- APScheduler (no Redis/Celery needed)
- Flask web interface
- Background scraping every 4 hours
- Completely portable - deploy anywhere

Run: python app.py
Access: http://localhost:5000
"""
from sqlalchemy.orm import joinedload
from flask import Flask, render_template, jsonify, send_from_directory, request
from flask_sqlalchemy import SQLAlchemy
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import scrapy
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor, defer
from crochet import setup, wait_for
import hashlib
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin
import os
import logging
from threading import Thread
import json
from urllib.parse import quote_plus
import time

import boto3
from botocore.exceptions import ClientError
from pypdf import PdfReader
import google.generativeai as genai

from dotenv import load_dotenv
load_dotenv()

import smtplib
from email.message import EmailMessage
from typing import Optional, List, Tuple

from extensions import db
from analysis_utils import extract_pdf_text, analyze_with_bedrock
from email_utils import get_alert_recipients, send_email, build_scrape_run_email




BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'compliance.db')

# Initialize Crochet for running Scrapy in Flask
setup()

# Flask app configuration
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{DB_PATH}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

"""Need to change the key or do smthn about it"""
app.config['SECRET_KEY'] = 'your-secret-key-change-in-production'

db.init_app(app)
from models import Regulation, RegulationAnalysis, ScrapeLog, PageHash


# Configure logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("spiders").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

import os
logger.info("SMTP_HOST=%s", os.getenv("SMTP_HOST"))
logger.info("SMTP_USER=%s", os.getenv("SMTP_USER"))
logger.info("ALERT_EMAILS=%s", os.getenv("ALERT_EMAILS"))


for noisy in [
    "scrapy.middleware",
    "scrapy.crawler",
    "scrapy.extensions",
    "scrapy.spiderloader",
    "scrapy.utils.log",
    "scrapy.statscollectors",
    "scrapy.core.scraper",
    "scrapy.core.engine",
    "twisted"
]:
    logging.getLogger(noisy).setLevel(logging.WARNING)

logger.info(f"Using database at: {DB_PATH}")

# Ensure downloads directory exists
os.makedirs('./downloads/pdfs', exist_ok=True)

# ============================================================================
# SCRAPING RUNNER
# ============================================================================

@wait_for(timeout=300)
def run_spiders(test_mode=False, secp_limit=5):

    from spiders import FBRSpider, SECPSpider, PCPGazetteSpider
    """Run all spiders - can be called from Flask routes or scheduler"""
    runner = CrawlerRunner({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_DELAY': 3,
        'LOG_LEVEL': 'INFO',
        'TWISTED_REACTOR': 'twisted.internet.selectreactor.SelectReactor',
        'TELNETCONSOLE_ENABLED': False,
    })
    
    @defer.inlineCallbacks
    def crawl():
        """There are two main spiders we have right now"""
        logger.info("FBRSpider: Starting crawl")
        yield runner.crawl(FBRSpider, flask_app=app)

        if test_mode:
            logger.info("SECPSpider: Test mode active - passing to crawler - SECP Spider (Stage 1)")
        logger.info("SECPSpider: Starting crawl")
        yield runner.crawl(SECPSpider, flask_app=app, test_mode=test_mode, limit=secp_limit)

        logger.info("PCPGazetteSpider: Starting crawl")
        yield runner.crawl(PCPGazetteSpider, flask_app=app, test_mode=test_mode)

        logger.info("All spiders completed")    
    

    
    return crawl()


def scrape_regulations(test_mode=False, secp_limit=5):
    """Main scraping function called by scheduler"""
    with app.app_context():
        log = ScrapeLog(started_at=datetime.utcnow(), status='running')
        db.session.add(log)
        db.session.commit()
        
        try:
            logger.info("Starting scraping cycle...")
            
            # Count before
            count_before = Regulation.query.count()
            
            # Run spiders
            spider_error = None
            try:
                run_spiders(test_mode=test_mode, secp_limit=secp_limit)
            except Exception as e:
                spider_error = e
                logger.error(f"Spider error: {e}. Spider failed to run. Continuing with rest of the process.")
            
            
            # Count after
            count_after = Regulation.query.count()
            new_downloads = count_after - count_before

            #Fetching the actual new regulations for this run
            new_regs = Regulation.query.filter(
                Regulation.discovered_at >= log.started_at
            ).order_by(Regulation.discovered_at.asc()).all()

            for reg in new_regs:
                reg.status = 'new'
            db.session.commit()


            analyzed_count = 0
            alerts_payload = []
            for reg in new_regs:
                analysis_obj = None

               # Only process if we actually downloaded the PDF
                if reg.file_path and os.path.exists(reg.file_path):
                    text = extract_pdf_text(reg.file_path)
                    if text:
                        analysis_obj = analyze_with_bedrock(text, reg)
                        if analysis_obj:
                            analyzed_count += 1
                        analyze_with_bedrock(text, reg, type="classify")
                else:
                    logger.info(
                        f"Skipping LLM analysis for regulation id:{reg.id}, ref_no:{reg.reference_number} (no PDF file_path)"
                    ) 

                alerts_payload.append((reg, analysis_obj))
            
            # Update log
            log.completed_at = datetime.utcnow()
            log.status = 'success'

            try:
                recipients = get_alert_recipients()
                if alerts_payload and recipients:
                    logger.info("About to send email alert for %s new regulations to %s recipients",
                                len(alerts_payload), len(recipients))
                    subject, body = build_scrape_run_email(alerts_payload, log)
                    send_email(subject, body, recipients)
                    logger.info(f"Sent scrape alert email to {len(recipients)} recipients.")
            except Exception as e:
                logger.error(f"Failed to send scrape alert email: {e}")

            log.regulations_found = count_after
            log.new_downloads = new_downloads
            db.session.commit()

            
            
            logger.info(f"Scraping completed. Found {new_downloads} new regulations, analyzed {analyzed_count} of them.")

            return new_downloads, analyzed_count
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            log.completed_at = datetime.utcnow()
            log.status = 'failed'
            log.error_message = str(e)
            db.session.commit()

            return 0, 0


def simulate_new_regulations(count=5, sources=("FBR", "SECP", "PCP")):
    with app.app_context():
        # 1) Get latest `count` regs by discovered_at
        regs = []
        for s in sources:
            regs.extend(
                Regulation.query
                .filter(Regulation.source == s)
                .order_by(
                    Regulation.issue_date.desc(),
                    Regulation.discovered_at.desc()
                )
                .limit(count)
                .all()
            )

        if not regs:
            logger.info("simulate_new_regulations: no regulations found to delete")
            return []

        reg_ids = [r.id for r in regs]

        logger.info(
            "simulate_new_regulations: preparing to delete %s regs: %s",
            len(reg_ids),
            reg_ids,
        )
        for r in regs:
            logger.info(
                "  -> id=%s, source=%s, ref=%s, issue_date=%s, discovered_at=%s",
                r.id, r.source, r.reference_number, r.issue_date, r.discovered_at,
            )

        # 2) Delete analyses for these regs (child table)
        deleted_analyses = RegulationAnalysis.query.filter(
            RegulationAnalysis.regulation_id.in_(reg_ids)
        ).delete(synchronize_session=False)

        logger.info(
            "simulate_new_regulations: deleted %s analyses for these regs",
            deleted_analyses,
        )

        # 3) Delete the regulations themselves (parent table)
        deleted_regs = Regulation.query.filter(
            Regulation.id.in_(reg_ids)
        ).delete(synchronize_session=False)

        logger.info(
            "simulate_new_regulations: deleted %s Regulation rows",
            deleted_regs,
        )

        # 4) Clear PageHash so Scrapy won't say "API unchanged - skipping"
        pagehash_deleted = PageHash.query.filter(
            PageHash.page_url.like("FBR:%")
        ).delete()
        logger.info(
            "simulate_new_regulations: deleted %s PageHash rows (forcing fresh scrape)",
            pagehash_deleted,
        )

        # 5) Commit changes
        db.session.commit()

        logger.info(
            "simulate_new_regulations: commit complete. "
            "Dashboard will temporarily show these regs as gone "
            "until scrape runs again."
        )

        return reg_ids



def analyze_recent_regulations(days=200, limit=10, reanalyze=True, mode="db", test_delete_count=5):
    """
    For testing: 

    mode="db"
        existing behavior
        pulls recent regs from db and sends to Bedrock.

    mode="scrape":
        - delete latest `test_delete_count` regs (and their analyses + page hashes)
          via simulate_new_regulations()
        - run scrape_regulations() to re-scrape and re-analyze them
        - return scrape metrics


    - days: look back N days from today based on issue_date
    - limit: max number of regs to analyze
    - reanalyze=False: only analyze regs with no RegulationAnalysis yet
    """
    if mode == "scrape":
        deleted_ids = simulate_new_regulations()#count==test_delete_count)
        #Doing this so that it reflects in the dashboard
        #I want the last 5 regulation to go from the dashboard, and then reappear after the scrape
   
        new_downloads, analyzed_count = scrape_regulations(test_mode=True, secp_limit=5)

        return {
            "mode": "scrape",
            "deleted_reg_ids": deleted_ids,
            "new_downloads": new_downloads,
            "analyzed_from_scrape": analyzed_count,
        }



    cutoff = datetime.utcnow().date() - timedelta(days=days)

    query = Regulation.query.filter(
        Regulation.issue_date != None,
        Regulation.issue_date >= cutoff,
        Regulation.file_path != None
    )

    if not reanalyze:
        # Only regs that do NOT already have an analysis
        query = query.filter(~Regulation.analyses.any())

    regs = query.order_by(Regulation.issue_date.desc()).limit(limit).all()

    logger.info(
        "Testing Bedrock: found %s recent regs (days=%s, limit=%s, reanalyze=%s)",
        len(regs), days, limit, reanalyze
    )

    analyzed = 0
    for reg in regs:
        text = extract_pdf_text(reg.file_path)
        if not text:
            logger.info(f"Skipping reg id:{reg.id}, ref_no:{reg.reference_number} (no text extracted)")
            continue

        analysis = analyze_with_bedrock(text, reg)
        analyze_with_bedrock(text, reg, type="classify")
        db.session.refresh(reg)
        if analysis:
            print(f"Analyzed reg id={reg.id}, ref_no={reg.reference_number} with Bedrock.\nDomain: {reg.domain}")
            print(analysis.summary, "\n-----\n")
            analyzed += 1

    logger.info("Testing Bedrock (DB Mode): analyzed %s regulations", analyzed)
    return analyzed, len(regs)




# ============================================================================
# FLASK ROUTES
# ============================================================================


@app.route('/')
def index():
    """Dashboard showing recent regulations"""
    recent = (
        Regulation.query
        .options(joinedload(Regulation.analyses))
        .order_by(
            Regulation.issue_date.desc(),
            Regulation.discovered_at.desc()
            )
            .limit(200)
            .all()
    )

    regs_for_view = list(recent)

    last_scrape = ScrapeLog.query.order_by(ScrapeLog.started_at.desc()).first()

    # "New" = discovered in the most recent scrape
    if last_scrape:
        new_count = Regulation.query.filter(
            Regulation.discovered_at >= last_scrape.started_at
        ).count()
    else:
        new_count = 0

    stats = {
        'total': Regulation.query.count(),
        'fbr': Regulation.query.filter_by(source='FBR').count(),
        'secp': Regulation.query.filter_by(source='SECP').count(),
        'sbp': Regulation.query.filter_by(source='SBP').count(),
        'pcp': Regulation.query.filter_by(source='PCP').count(),
        'new': new_count, #Regulation.query.filter_by(status='new').count(),
    }
    
    #last_scrape = ScrapeLog.query.order_by(ScrapeLog.started_at.desc()).first()
    
    return render_template('dashboard.html', 
                         regulations=regs_for_view, 
                         stats=stats,
                         last_scrape=last_scrape)


@app.route('/api/regulations')
def api_regulations():
    """API endpoint for regulations"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    source = request.args.get('source', None)
    
    query = Regulation.query
    
    if source:
        query = query.filter_by(source=source)
    
    regulations = query.order_by(Regulation.discovered_at.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    return jsonify({
        'regulations': [r.to_dict() for r in regulations.items],
        'total': regulations.total,
        'pages': regulations.pages,
        'current_page': page
    })


@app.route('/api/scrape/trigger', methods=['POST'])
def trigger_scrape():
    """Manually trigger a scrape"""
    Thread(target=scrape_regulations).start()
    return jsonify({'status': 'started', 'message': 'Scraping started in background'})


@app.route('/api/stats')
def api_stats():
    """Get statistics"""
    return jsonify({
        'total_regulations': Regulation.query.count(),
        'by_source': {
            'FBR': Regulation.query.filter_by(source='FBR').count(),
            'SECP': Regulation.query.filter_by(source='SECP').count(),
            'SBP': Regulation.query.filter_by(source='SBP').count(),
            'PCP': Regulation.query.filter_by(source='PCP').count(),
        },
        'by_status': {
            'new': Regulation.query.filter_by(status='new').count(),
            'processed': Regulation.query.filter_by(status='processed').count(),
        },
        'recent_scrapes': [
            {
                'started': log.started_at.isoformat(),
                'completed': log.completed_at.isoformat() if log.completed_at else None,
                'status': log.status,
                'new_downloads': log.new_downloads
            }
            for log in ScrapeLog.query.order_by(ScrapeLog.started_at.desc()).limit(10).all()
        ]
    })

@app.route('/api/analyze_recent', methods=['POST'])
def api_analyze_recent():
    """
    Trigger Bedrock analysis for recent regulations from the dashboard.

    For now, this runs synchronously and returns how many were analyzed.
    Keep 'limit' small while testing so the UI doesn't hang too long.
    """
    data = request.get_json(silent=True) or {}

    mode = data.get("mode", "db")

    if mode == "scrape":
        test_delete_count = int(data.get("test_delete_count", 5))

        result = analyze_recent_regulations(
            mode="scrape",
            test_delete_count=test_delete_count
        )
        return jsonify({
            "status": "ok",
            **result,
        })

    days = int(data.get('days', 200))      # look back N days
    limit = int(data.get('limit', 10))     # max docs per run
    reanalyze = bool(data.get('reanalyze', True))

    analyzed, total = analyze_recent_regulations(
        days=days,
        limit=limit,
        reanalyze=reanalyze,
        mode="db"
    )

    return jsonify({
        "status": "ok",
        "mode": "db",
        "days": days,
        "limit": limit,
        "total_candidates": total,
        "analyzed_now": analyzed,
        "reanalyze": reanalyze,
    })



@app.route('/downloads/<path:filename>')
def download_file(filename):
    """Serve downloaded PDFs"""
    return send_from_directory('./downloads/pdfs', filename)


# ============================================================================
# SCHEDULER SETUP
# ============================================================================

def init_scheduler():
    """Initialize APScheduler for background scraping"""
    scheduler = BackgroundScheduler()
    
    # Schedule scraping every 4 hours
    scheduler.add_job(
        func=scrape_regulations,
        trigger=CronTrigger(hour='8,12,16,20,2'),  # 8 AM, 12 PM, 4 PM, 8 PM, 2 AM
        id='scrape_regulations',
        name='Scrape regulatory websites',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started - scraping every 4 hours")
    
    return scheduler


# ============================================================================
# APP INITIALIZATION
# ============================================================================

if __name__ == '__main__':
    with app.app_context():
        # Create database tables
        db.create_all()
        logger.info("Database initialized")
        
        # Start scheduler
        scheduler = init_scheduler()
        
        # Run Flask app
        logger.info("Starting Flask app on http://localhost:5000")
        app.run(debug=True, use_reloader=False)