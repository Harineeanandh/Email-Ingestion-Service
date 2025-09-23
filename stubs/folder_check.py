import os

# Replace this with the path to your dataset root
dataset_path = "maildir"

def list_users_and_folders(dataset_path):
    if not os.path.exists(dataset_path):
        print("Dataset path does not exist.")
        return

    users = [u for u in os.listdir(dataset_path) if os.path.isdir(os.path.join(dataset_path, u))]
    
    for user in users:
        user_path = os.path.join(dataset_path, user)
        print(f"User: {user}")
        
        folders = [f for f in os.listdir(user_path) if os.path.isdir(os.path.join(user_path, f))]
        if not folders:
            print("  No folders found")
        else:
            for folder in folders:
                folder_path = os.path.join(user_path, folder)
                email_count = len([e for e in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, e))])
                print(f"  Folder: {folder} | Emails: {email_count}")

if __name__ == "__main__":
    list_users_and_folders(dataset_path)
