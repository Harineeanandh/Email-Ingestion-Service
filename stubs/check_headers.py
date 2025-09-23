import os
import email

# Configuration
dataset_path = "maildir"        # Your dataset root
test_user = "shively-h"         # Pick any user
test_folder = "inbox"           # Pick any folder
num_emails_to_read = 3          # Number of emails to sample

# Path to the folder to test
folder_path = os.path.join(dataset_path, test_user, test_folder)

# List all files in the folder
email_files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

print(f"Found {len(email_files)} emails in {test_user}/{test_folder}")
print(f"Showing first {num_emails_to_read} emails:\n")

for i, email_file in enumerate(email_files[:num_emails_to_read]):
    email_path = os.path.join(folder_path, email_file)
    
    with open(email_path, "r", encoding="utf-8", errors="ignore") as f:
        msg = email.message_from_file(f)
        
        print(f"--- Email {i+1} ---")
        print("From:", msg.get("From"))
        print("To:", msg.get("To"))
        print("Subject:", msg.get("Subject"))
        print("Date:", msg.get("Date"))
        
        # Get body snippet
        body_snippet = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    body_snippet = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                    break
        else:
            body_snippet = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
        
        print("Body snippet:", body_snippet[:200].replace("\n", " "))  # first 200 chars
        print("\n")
