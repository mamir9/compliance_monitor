import os
import logging
import json
import re

import boto3
from botocore.exceptions import ClientError
from pypdf import PdfReader
import google.generativeai as genai

from extensions import db
from models import Regulation, RegulationAnalysis

logger = logging.getLogger(__name__)


BEDROCK_REGION = os.getenv('BEDROCK_REGION', os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
BEDROCK_MODEL_ID = os.getenv(
    'BEDROCK_MODEL_ID',
    'anthropic.claude-3-haiku-20240307-v1:0'  # example, change to whatever you enabled
)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL_ID = os.getenv("GEMINI_MODEL_ID", "gemini-2.5-pro")  # or another Gemini model

def get_gemini_model():
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not set; Gemini OCR disabled")
        return None
    genai.configure(api_key=GEMINI_API_KEY)
    try:
        return genai.GenerativeModel(GEMINI_MODEL_ID)
    except Exception as e:
        logger.error(f"Failed to initialize Gemini model: {e}")
        return None

gemini_model = get_gemini_model()


def get_bedrock_client():
    return boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

bedrock_client = get_bedrock_client()



def needs_ocr(extracted_text: str) -> bool:
    """
    Decide whether we should fall back to OCR.
    Tries to be conservative so we don't override good PyPDF output.
    """
    if not extracted_text:
        return True

    text = extracted_text.strip()
    if len(text) < 200:
        # Likely just a header / watermark / 'CamScanner'
        return True

    # If most characters are not letters/numbers/spaces, it's probably garbage
    total = len(text)
    clean = ''.join(ch for ch in text if ch.isalnum() or ch.isspace())
    clean_ratio = len(clean) / total if total > 0 else 0

    if clean_ratio < 0.4:  # lots of weird symbols
        return True

    # Explicit CamScanner-type watermark case
    if "camscanner" in text.lower() and len(text) < 1000:
        return True

    return False


def clean_for_bedrock(text: str, max_chars: int = 8000) -> str:
    """
    Clean up document text before sending to Bedrock:
    - Remove problematic control characters
    - Keep newlines and tabs
    - Truncate to max_chars
    """
    if not text:
        return ""

    # Replace control chars (except newline/tab) with spaces
    cleaned_chars = []
    for ch in text:
        code = ord(ch)
        if ch in ("\n", "\t"):
            cleaned_chars.append(ch)
        elif code < 32 or code == 127:
            cleaned_chars.append(" ")
        else:
            cleaned_chars.append(ch)

    cleaned = "".join(cleaned_chars)

    # Optional: collapse huge runs of whitespace
    cleaned = " ".join(cleaned.split())

    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars]

    return cleaned.strip()



def extract_pdf_text_with_gemini(file_path, max_chars=12000):
    """
    Use Gemini (multimodal) as an OCR engine for image-only PDFs.

    Sends the PDF bytes and asks for plain text extraction.
    """
    if not gemini_model:
        logger.warning("Gemini model not available; skipping OCR")
        return None

    if not file_path or not os.path.exists(file_path):
        logger.warning(f"Gemini OCR called with missing file: {file_path}")
        return None

    try:
        with open(file_path, "rb") as f:
            pdf_bytes = f.read()

        prompt = (
            "Extract the full machine-readable text from this scanned PDF. "
            "Return plain text only, no comments or explanation."
        )

        # Gemini 1.5 models can take PDF bytes directly
        response = gemini_model.generate_content(
            [
                prompt,
                {
                    "mime_type": "application/pdf",
                    "data": pdf_bytes,
                },
            ]
        )

        text = (response.text or "").strip()
        if not text:
            logger.warning(f"Gemini OCR returned empty text for {file_path}")
            return None

        if len(text) > max_chars:
            text = text[:max_chars]

        logger.info(f"Gemini OCR extracted {len(text)} chars from {file_path}")
        return text

    except Exception as e:
        logger.error(f"Gemini OCR failed for {file_path}: {e}")
        return None



def extract_pdf_text(file_path, max_chars=12000, use_ocr=True):
    if not file_path or not os.path.exists(file_path):
        return None
    try:
        reader = PdfReader(file_path)
        text_chunks = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            text_chunks.append(page_text)

            #Should we just abort? Because there are probably going
            #To be allot of those huge docs - what should be the strategy
            #to handle them? Even for summarizing we would need to call an llm
            # - could chunk maybe? But what if we miss important info? How
            # would we retrieve what's important in that document?

            #Wait how much are we taking here? what's the limit we're breaking at?
            #Oh ok here's we're cutting if the accumulated text so far exceeds max_chars
            if sum(len(c) for c in text_chunks) >= max_chars:
                break
        full_text = "\n\n".join(text_chunks)
        if len(full_text) > max_chars:
            full_text = full_text[:max_chars]

    except Exception as e:
        logger.error(f"Failed to extract text from {file_path} with PyPDF: {e}")
        full_text = ""

    if full_text and not needs_ocr(full_text):
        return full_text.strip()
    
    if not use_ocr:
        return full_text.strip() or None


    logger.info(
        f"PyPDF extraction looked weak for {file_path} "
        f"(len={len(full_text)}); attempting Gemini OCR..."
    )

    ocr_text = extract_pdf_text_with_gemini(file_path, max_chars=max_chars)

    # 4) Prefer OCR text if we got something; otherwise fall back to PyPDF result
    if ocr_text:
        return ocr_text


    return full_text or None
   


def extract_domain_from_summary(text: str):
    if not text:
        return None
    
    # tolerant matching
    m = re.search(r'Domain\s*:\s*([A-Za-z \-/&]+)', text, re.IGNORECASE)
    if not m:
        return None

    domain = m.group(1).strip()
    return domain[:100] if domain else None



def analyze_with_bedrock(document_text, regulation: Regulation, type = "identify"):
    #Sends reg doc text to bedrock and stores summary
    if not document_text:
        return None
    
    document_text = clean_for_bedrock(document_text, max_chars=8000)
    if not document_text:
        logger.warning(f"Bedrock: cleaned document text is empty for regulation id={regulation.id}")
        return None
    
    prompt = f"""
You are a Pakistani tax and corporate compliance assistant.

You are given the full or partial text of an official regulation or SRO.
1. Identify the **main purpose** of this document.
2. Summarise the **key changes / obligations** in bullet points.
3. Highlight **who is affected** (e.g. individuals, companies, sectors).
4. Note any **effective dates** if visible.

Respond in concise English, suitable for internal compliance tracking.

Document title: {regulation.title}
Reference: {regulation.reference_number}
Source: {regulation.source}
Issue date: {regulation.issue_date}

--- BEGIN DOCUMENT TEXT ---
{document_text}
--- END DOCUMENT TEXT ---
    """.strip()

    identify_prompt = f"""
You are a Pakistani Tax and Corporate Compliance Assistant.

You are given the full or partial text of an offical regulation or SRO. You must operate in two phases:

First - Extraction phase:
First look carefully and identify two dates: when this document was issued, and when it becomes effective.
Then scan the document to extract the reference number.

To support this process, you are given the document title, reference number, source, and issue date
You must scan the text to verify that these are correct, and if so, fill them in the output below. 
For Reference number, only use the given one as a backup. First try to find SRO number in document text and give full form.

Document title: {regulation.title}
Reference: {regulation.reference_number}
Source: {regulation.source}
Issue date: {regulation.issue_date}

Second - General Idea and Impact phase:
Identify the **main purpose** of this document, then summarise **key changes/obligation**. Lastly, highlight **who is affected** (e.g. individuals, companies, sectors).


Now return all this information in the following format:
1. Subject:[Regulatory Alert] FBR/IFRS Update — [Short Title] — [Effective Date]
2. Source: [e.g., Federal Board of Revenue (FBR) / International Accounting Standards Board (IASB)]
3. Date Issued: [YYYY-MM-DD]
4. Effective Date: [YYYY-MM-DD or Immediate]
5. Document Type: [SRO / Circular / IFRS Amendment / SBP Circular etc.]
6. Reference Number: [Official SRO Number, first look for SRO and give full form e.g. S.R.O.1437(I)/2025, if not found revert to given reference no., if not applicable, state "N/A"]

7. General Idea: [Concise summary of main purpose, key changes/obligations]

8. Impact: [Brief analysis of compliance implications and affected parties]

If you are not given the regulation document then abort and say "No document text provided".
If there's anything you cannot find, write N/A for that field. Do not fill in anything by guesswork.

--- BEGIN DOCUMENT TEXT ---
{document_text}
--- END DOCUMENT TEXT ---
    """.strip()

    impact_prompt = f"""
You are a Pakistani Tax and Corporate Compliance Assistant, for the company Interloop Holdings.
You will be given information about a regulation, as well as information about the company you advise.
Your task is to analyze the regulation and determine if it impacts the company, and how much it impacts them.


Regulation Information:


    """
    classify_prompt = f"""
    You are a Pakistani Tax and Corporate Compliance Assistant.
    You will be given a regulation document, and your task is to classify it by domain.
    Possible domains: Taxation, Accounting Standard, Compliance, Financial Reporting, Corporate Law, Other.

    Give your answer in the following format:
    Domain: [one of the possible domains]

    Regulation Document: {regulation.title}

    --- BEGIN DOCUMENT TEXT ---
    {document_text}
    --- END DOCUMENT TEXT ---
    """.strip()

    #Swap out the prompt in the body for different analyses types
    if type == "identify":
        used_prompt = identify_prompt
    elif type == "impact":
        used_prompt = impact_prompt
    elif type == "classify":
        used_prompt = classify_prompt


    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "temperature": 0.2,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": used_prompt}
                ],
            }
        ],
    }

    try:
        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json",
        )
        resp_body = json.loads(response["body"].read())

        # Find first text block
        text_blocks = [
            c["text"] for c in resp_body.get("content", [])
            if c.get("type") == "text"
        ]
        summary_text = "\n".join(text_blocks).strip() if text_blocks else ""

        if not summary_text:
            logger.warning("Bedrock returned no text content for regulation %s", regulation.id)
            return None
        
        if type == "classify":
            domain = extract_domain_from_summary(summary_text)
            if domain:
                regulation.domain = domain
            db.session.commit()
            return None


        analysis = RegulationAnalysis(
            regulation_id=regulation.id,
            model_id=BEDROCK_MODEL_ID,
            summary=summary_text,
            raw_response=json.dumps(resp_body),
        )
        db.session.add(analysis)

        if type == "classify":
            domain = extract_domain_from_summary(summary_text)
            if domain:
                regulation.domain = domain
            
        db.session.commit()

        logger.info(f"Stored Bedrock analysis {type} for regulation id={regulation.id}, reference_number:{regulation.reference_number}")
        return analysis

    except ClientError as e:
        logger.error(f"Bedrock ClientError: {e}")
    except Exception as e:
        logger.error(f"Unexpected error when calling Bedrock: {e}")

    return None
