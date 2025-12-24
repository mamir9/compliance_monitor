import os

# ✅ MUST be installed before Scrapy imports reactor
from scrapy.utils.reactor import install_reactor
install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

from flask import Flask
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from extensions import db
from pc_pwspider import PakistanCodePlaywrightSpider  # <-- change to your filename/class


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "compliance.db")


def main():
    # Flask + DB context for your BaseSpider DB helpers
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DB_PATH}"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = "dev"

    db.init_app(app)

    settings = get_project_settings()
    settings.set("LOG_LEVEL", "INFO")
    settings.set("TELNETCONSOLE_ENABLED", False)

    # optional: see playwright logs if needed
    # settings.set("PLAYWRIGHT_LOG_LEVEL", "debug")

    process = CrawlerProcess(settings)

    process.crawl(
        PakistanCodePlaywrightSpider,
        flask_app=app,
        test_mode=True,          # only page 1
        year=2025,
        actions="inactive,active",
        max_pages=1,
        recency_days=365,
    )
    process.start()


if __name__ == "__main__":
    main()
