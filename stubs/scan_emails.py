import os

dataset_path = "maildir"
attachments_found = 0

for root, dirs, files in os.walk(dataset_path):
    for file in files:
        email_path = os.path.join(root, file)
        try:
            with open(email_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if "Content-Disposition:" in line and "attachment" in line.lower():
                        attachments_found += 1
                        print(f"Attachment header found in: {email_path}")
                        break
        except Exception as e:
            print(f"Error reading {email_path}: {e}")

print(f"\nTotal emails with real attachment headers: {attachments_found}")
