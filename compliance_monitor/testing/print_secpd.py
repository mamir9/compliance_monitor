# print_secp_by_issue_date.py
import os
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from datetime import datetime

from app import app, db  # assumes app.py defines `app` and `db`
from models import Regulation  # assumes models.py defines Regulation


def fmt(dt):
    if not dt:
        return "None"
    # dt might already be a datetime; if it's a string, just print it
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


def main(limit=20):
    with app.app_context():
        regs = (
            Regulation.query
            .filter(Regulation.source == "SECP")
            .order_by(
                Regulation.issue_date.desc(),        # primary sort you requested
                Regulation.discovered_at.desc(),     # tie-breaker
                Regulation.id.desc()                 # tie-breaker
            )
            .limit(limit)
            .all()
        )

        total = Regulation.query.filter(Regulation.source == "SECP").count()
        print(f"Total SECP regs: {total}")
        print(f"Printing top {min(limit, total)} sorted by issue_date desc:\n")

        for r in regs:
            print(
                f"id={r.id} | "
                f"reg_id={r.regulation_id} | "
                f"ref={r.reference_number} | "
                f"issue_date={r.issue_date} | "
                f"discovered_at={fmt(r.discovered_at)} | "
                f"status={r.status} | "
                f"url={r.page_url}"
            )


if __name__ == "__main__":
    main(limit=20)
