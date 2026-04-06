import os
import imaplib
import email
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("APP_PASSWORD")
IMAP_SERVER = "imap.gmail.com"
CLIENT_EMAIL = "tristenhunter0702@gmail.com"

def get_unique_filename(folder, filename):
    """Prevents overwriting by adding (1), (2), etc. to duplicate filenames."""
    base, extension = os.path.splitext(filename)
    counter = 1
    unique_path = os.path.join(folder, filename)
    
    while os.path.exists(unique_path):
        unique_path = os.path.join(folder, f"{base} ({counter}){extension}")
        counter += 1
        
    return unique_path

def extract_date_from_filename(filename):
    """
    Rips the date out of filenames like '23.03.2026' or '23 MARCH 2026'.
    Returns (year, month_name, day_folder)
    """
    # Pattern 1: DD.MM.YYYY (e.g., 23.03.2026)
    dot_match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", filename)
    if dot_match:
        day, month, year = dot_match.groups()
        month_name = datetime.strptime(month, "%m").strftime("%B")
        day_folder = f"{day} {month_name[:3]}"
        return year, month_name, day_folder

    # Pattern 2: DD MONTH YYYY (e.g., 23 MARCH 2026)
    space_match = re.search(r"(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})", filename)
    if space_match:
        day, month_str, year = space_match.groups()
        try:
            # Standardize month name
            month_date = datetime.strptime(month_str.capitalize(), "%B")
            month_name = month_date.strftime("%B")
            day_folder = f"{int(day):02d} {month_name[:3]}"
            return year, month_name, day_folder
        except ValueError:
            pass

    return None, None, None

def get_target_folders(filename):
    """Determines where the file goes based on its name, fallback to 'Today'."""
    year, month, day_folder = extract_date_from_filename(filename)
    
    if not year:
        now = datetime.now()
        year, month, day_folder = now.strftime("%Y"), now.strftime("%B"), now.strftime("%d %b")

    raw_path = os.path.join(year, month, day_folder, "raw_data")
    report_path = os.path.join(year, month, day_folder, "reports")
    
    os.makedirs(raw_path, exist_ok=True)
    os.makedirs(report_path, exist_ok=True)
    
    return raw_path

def download_attachments(mail, sender_email):
    mail.select("inbox")
    # Search for UNSEEN emails from your client
    status, messages = mail.search(None, f'(UNSEEN FROM "{sender_email}")')
    email_ids = messages[0].split()

    print(f"Found {len(email_ids)} new emails from {sender_email}.")

    for eid in email_ids:
        status, msg_data = mail.fetch(eid, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                
                for part in msg.walk():
                    # Only download actual attachments
                    if part.get_content_disposition() == "attachment":
                        filename = part.get_filename()
                        if filename:
                            # 1. Find the correct folder based on the filename's date
                            target_raw_folder = get_target_folders(filename)
                            
                            # 2. Get a unique path (no overwriting)
                            final_path = get_unique_filename(target_raw_folder, filename)
                            
                            with open(final_path, "wb") as f:
                                f.write(part.get_payload(decode=True))
                            
                            print(f"  -> Saved: {os.path.relpath(final_path)}")
        
        # Mark as read so we don't process it again
        mail.store(eid, '+FLAGS', '\\Seen')

def main():
    try:
        print("Connecting...")
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL, PASSWORD)
        download_attachments(mail, CLIENT_EMAIL)
        mail.logout()
        print("\nSync Complete. Files organized by Data Date.")
    except Exception as e:
        print(f"Critical Error: {e}")

if __name__ == "__main__":
    main()