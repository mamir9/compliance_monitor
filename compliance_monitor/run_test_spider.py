from scrapy.utils.reactor import install_reactor
install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Import your Flask app factory / app object (adjust these imports)
from flask import Flask
from spider_test import PakistanCodeSpider  # adjust path
from extensions import db

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'compliance.db')



def main():

    app = Flask(__name__)
    print("\n\n\nDatabase path:", DB_PATH, "\n\n\n")
    app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{DB_PATH}"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = 'your-secret-key-change-in-production'

    db.init_app(app)

    settings = get_project_settings()
    settings.set("LOG_LEVEL", "INFO")
    settings.set("LOGSTATS_INTERVAL", 10)
    settings.set("ROBOTSTXT_OBEY", False)
    settings.set("TELNETCONSOLE_ENABLED", False)
    #settings.set("TWISTED_REACTOR", "twisted.internet.selectreactor.SelectReactor")

    process = CrawlerProcess(settings)

    process.crawl(
        PakistanCodeSpider,
        flask_app=app,
        test_mode=True,          # page 1 only
        year=2025,
        actions="inactive,active",
        max_pages=2,
    )
    process.start()

if __name__ == "__main__":
    main()