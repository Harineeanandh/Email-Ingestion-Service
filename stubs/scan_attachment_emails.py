import os
import email
from email import policy

dataset_path = "maildir"

emails_with_attachments = 0       # emails that declare an attachment
emails_with_real_data = 0         # emails that have actual attachment payload
emails_with_only_headers = 0      # emails with attachment header but no real data

details = []

for root, dirs, files in os.walk(dataset_path):
    for file in files:
        email_path = os.path.join(root, file)
        has_attachment_header = False
        has_real_attachment = False

        try:
            with open(email_path, "r", encoding="latin-1") as f:
                msg = email.message_from_file(f, policy=policy.default)

            for part in msg.walk():
                if part.get_content_disposition() == "attachment":
                    has_attachment_header = True
                    filename = part.get_filename()
                    payload = part.get_payload(decode=True)

                    if payload:
                        has_real_attachment = True
                        size = len(payload)
                        details.append((email_path, filename, size))
                    else:
                        # attachment header exists but no actual data
                        details.append((email_path, filename, 0))

        except Exception as e:
            print(f"Error parsing {email_path}: {e}")
            continue

        # increment counters based on what we found
        if has_attachment_header:
            emails_with_attachments += 1
            if has_real_attachment:
                emails_with_real_data += 1
            else:
                emails_with_only_headers += 1

print("\n==== Attachment Summary ====")
print(f"Total emails with attachment headers: {emails_with_attachments}")
print(f"Emails with real attachment data: {emails_with_real_data}")
print(f"Emails with only headers / empty attachments: {emails_with_only_headers}")

print("\nSample (first 10 attachments with real data):")
for d in details[:10]:
    print(f"Email: {d[0]} | File: {d[1]} | Size: {d[2]} bytes")
