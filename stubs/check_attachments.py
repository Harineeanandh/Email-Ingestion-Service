import os
import re
import email
from email import policy

dataset_path = "maildir"            # root of your maildir
max_examples_per_folder = 3        # how many attachment examples to print per folder

# Regex patterns to catch attachment headers in raw text
PATTERNS = [
    re.compile(r'Content-Disposition:\s*attachment\s*;?\s*filename\s*=\s*"([^"]+)"', re.I),
    re.compile(r'Content-Disposition:\s*attachment\s*;?\s*filename\s*=\s*([^;\r\n]+)', re.I),
    re.compile(r'Content-Type:[^\r\n]*\bname\s*=\s*"([^"]+)"', re.I),
    re.compile(r'Content-Type:[^\r\n]*\bname\s*=\s*([^;\r\n]+)', re.I),
    re.compile(r'filename\*?=\s*(?:UTF-8\'\')?"?([^"\r\n;]+)"?', re.I),
    re.compile(r'X-MS-Has-Attach:\s*(\S*)', re.I)
]

def extract_from_parser(msg):
    """
    Use the email parser to find attachments (filenames or content-type indicators).
    Returns a set of attachment names / indicators.
    """
    found = set()
    try:
        # Use policy=default to improve parsing of real-world emails
        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue

            filename = part.get_filename()
            cdisp = (part.get("Content-Disposition") or "")
            ctype = (part.get_content_type() or "").lower()

            if filename:
                found.add(filename)
            elif 'attachment' in cdisp.lower():
                # try to extract filename from Content-Disposition header
                m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cdisp, re.I)
                if m:
                    found.add(m.group(1))
                else:
                    found.add("(attachment)")
            elif ctype.startswith(("application/", "image/", "video/")) and filename is None:
                # possible attachment without filename
                name_param = part.get_param('name')  # fallback
                if name_param:
                    found.add(name_param)
                else:
                    found.add(f"({ctype})")
    except Exception:
        # be tolerant of parse errors
        pass
    return found

def extract_from_raw(raw_text):
    """
    Fallback: scan raw file text for attachment-like header patterns and extract filenames.
    Returns a set of attachment names / indicators.
    """
    found = set()
    for pat in PATTERNS:
        for m in pat.findall(raw_text):
            name = m.strip().strip('"').strip()
            if name:
                found.add(name)
    return found

def detect_attachments_in_file(email_path):
    """
    Returns a set of attachment names (could be placeholders like '(attachment)' or content-type indicators).
    """
    try:
        with open(email_path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
    except Exception:
        return set()

    # 1) Parse using email parser (more structured)
    try:
        # parse from string using a tolerant policy
        msg = email.message_from_string(raw, policy=policy.default)
        found = extract_from_parser(msg)
    except Exception:
        found = set()

    # 2) If parser found nothing, or to double-check, scan raw text
    raw_found = extract_from_raw(raw)
    found.update(raw_found)

    return found

def scan_dataset(dataset_path, max_examples_per_folder=3):
    total_with_attachments = 0
    for user in sorted(os.listdir(dataset_path)):
        user_path = os.path.join(dataset_path, user)
        if not os.path.isdir(user_path):
            continue

        for folder in sorted(os.listdir(user_path)):
            folder_path = os.path.join(user_path, folder)
            if not os.path.isdir(folder_path):
                continue

            examples_printed = 0
            for email_file in os.listdir(folder_path):
                email_path = os.path.join(folder_path, email_file)
                if not os.path.isfile(email_path):
                    continue

                attachments = detect_attachments_in_file(email_path)
                if attachments:
                    total_with_attachments += 1
                    if examples_printed < max_examples_per_folder:
                        print(f"User: {user} | Folder: {folder} | Email: {email_file}")
                        print("  Attachments found:", ", ".join(sorted(attachments)))
                        print("-" * 60)
                        examples_printed += 1
            # optional: small progress indicator per folder
    print(f"Total emails with attachments found: {total_with_attachments}")

if __name__ == "__main__":
    scan_dataset(dataset_path, max_examples_per_folder)
