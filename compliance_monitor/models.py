from datetime import datetime
from extensions import db
import re

class PageHash(db.Model):
    """Stores content hash of scraped pages for change detection"""
    __tablename__ = 'page_hashes'
    
    id = db.Column(db.Integer, primary_key=True)
    page_url = db.Column(db.String(500), unique=True, nullable=False, index=True)
    content_hash = db.Column(db.String(64), nullable=False)  # SHA256 hash
    last_checked = db.Column(db.DateTime, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class RegulationAnalysis(db.Model):
    """LLM analysis of a regulation document"""
    __tablename__ = 'regulation_analyses'

    id = db.Column(db.Integer, primary_key=True)
    regulation_id = db.Column(db.Integer, db.ForeignKey('regulations.id'), nullable=False)
    model_id = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    summary = db.Column(db.Text, nullable=False)
    raw_response = db.Column(db.Text)  # optional full JSON if you want

    regulation = db.relationship('Regulation', backref='analyses')

    # @property
    def general_idea_and_impact(self):
        """
        Return only the part of the LLM output from:
        '7. General Idea:' onwards (including 8. Impact).
        If we can't find '7.' we fall back to full summary.
        """
        if not self.summary:
            return None

        text = self.summary

        # Try to find the exact "7. General Idea" marker first
        idx = text.find("7. General Idea")
        if idx == -1:
            # Fallback: any "7." marker
            idx = text.find("7.")
        if idx == -1:
            # Last fallback: just show everything
            return text.strip()

        return text[idx:].strip()
    
    @property
    def gi_and_impact(self):
        """
        Extract ONLY the part after '7.' until the end.
        Handles:
        7. General Idea: ...
        8. Impact: ...
        """
        if not self.summary:
            return None
        
        # look for section starting at "7."
        m = re.search(r"7\.\s*(.*)", self.summary, re.DOTALL)
        if not m:
            return None
        
        extracted = m.group(1).strip()
        return extracted
    
    @property
    def general_idea(self):
        """
        Extract text between:
        7. ...  (General Idea)
        and
        8. ...  (Impact)
        """
        if not self.summary:
            return None
        
        # Capture text between 7. and 8.
        m = re.search(r"7\.\s*General Idea:\s*(\S[\s\S]*?)(?=\n\s*8\.\s*Impact:)", self.summary, re.DOTALL)
        if not m:
            return None
        
        return m.group(1).strip()

    @property
    def impact(self):
        """
        Extract text from:
        8. ... (Impact)
        to end of summary.
        """
        if not self.summary:
            return None
        
        m = re.search(r"8\.\s*Impact:\s*(\S[\s\S]*)", self.summary, re.DOTALL)
        if not m:
            return None
        
        return m.group(1).strip()



class Regulation(db.Model):
    """Database model for regulations"""
    __tablename__ = 'regulations'
    
    id = db.Column(db.Integer, primary_key=True)
    """unique identifier - hash of source + ref + date"""
    regulation_id = db.Column(db.String(32), unique=True, nullable=False, index=True)
    source = db.Column(db.String(50), nullable=False, index=True)
    page_url = db.Column(db.Text, nullable=False)
    document_url = db.Column(db.Text)
    """Official ID - will it check against this or regulation_id?
    - checks regulation id
    """
    reference_number = db.Column(db.String(100), nullable=False)
    title = db.Column(db.Text, nullable=False)
    issue_date = db.Column(db.Date)
    effective_date = db.Column(db.Date)

    category = db.Column(db.String(100))
    document_type = db.Column(db.String(50))

    domain = db.Column(db.String(100)) # e.g. Taxation, Accounting Standard, etc.
    
    content_hash = db.Column(db.String(64))
    file_path = db.Column(db.Text)
    status = db.Column(db.String(20), default='new')
    discovered_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    last_checked = db.Column(db.DateTime)
    
    def to_dict(self):
        return {
            'id': self.id,
            'regulation_id': self.regulation_id,
            'source': self.source,
            'reference_number': self.reference_number,
            'title': self.title,
            'issue_date': self.issue_date.isoformat() if self.issue_date else None,
            'category': self.category,
            'document_type': self.document_type,
            'domain': self.domain,
            'status': self.status,
            'discovered_at': self.discovered_at.isoformat() if self.discovered_at else None,
            'file_path': self.file_path
        }
    @property
    def latest_analysis(self):
        """Convenience: most recent LLM analysis for this regulation."""
        if not self.analyses:
            return None
        # analyses are usually few, so simple max() is fine
        return max(self.analyses, key=lambda a: a.created_at)


class ScrapeLog(db.Model):
    """Log of scraping runs"""
    __tablename__ = 'scrape_logs'
    
    id = db.Column(db.Integer, primary_key=True)
    started_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)
    status = db.Column(db.String(20))  # success, failed, running
    regulations_found = db.Column(db.Integer, default=0)
    new_downloads = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text)

"""
This is tracking each time the scraper runs, how many new 
 regulations it found, any errors etc.
 Where is this record being stored? In SQLite? 
 """
