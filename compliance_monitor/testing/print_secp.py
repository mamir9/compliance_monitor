import os
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from app import app
from models import Regulation

with app.app_context():
    regs = (
        Regulation.query
        .filter_by(source="SECP")
        .order_by(Regulation.discovered_at.desc())
        .all()
    )

    print(f"Total SECP regs: {len(regs)}\n")
    for r in regs[:200]:  # change limit as needed
        print(
            f"\n\nid={r.id} | reg_id={r.regulation_id} | ref={r.reference_number} | "
            f"issue_date={r.issue_date} | status={r.status} | url={r.document_url} | "
            f"discovered_at={r.discovered_at} | title={r.title[:100]}..."
        )
