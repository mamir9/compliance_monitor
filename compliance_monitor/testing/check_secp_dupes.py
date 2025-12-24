import os
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from sqlalchemy import func

from app import app
from extensions import db
from models import Regulation

TARGET = "SECP-WPDM-52711"   # pick one from your dupes list

with app.app_context():
    rows = (
        Regulation.query
        .filter(Regulation.source == "SECP", Regulation.reference_number == TARGET)
        .order_by(Regulation.discovered_at.asc())
        .all()
    )

    print(f"Found {len(rows)} rows for {TARGET}")
    for r in rows:
        print(
            f"\n\nid={r.id} | regulation_id={r.regulation_id} | issue_date={r.issue_date} "
            f"| discovered_at={r.discovered_at} | doc_url={r.document_url} | file={bool(r.file_path)}"
        )