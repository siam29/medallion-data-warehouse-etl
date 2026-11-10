import os, sqlite3, hashlib, secrets, getpass

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auth.db")
ITER = 310_000

if not os.path.isfile(DB):
    raise SystemExit("auth.db not found next to this script: " + DB)

con = sqlite3.connect(DB)

def show():
    rows = con.execute("SELECT username, role, status FROM app_users ORDER BY user_id").fetchall()
    print("\n%-20s %-8s %s" % ("USERNAME", "ROLE", "STATUS"))
    for r in rows:
        print("%-20s %-8s %s" % r)
    return rows

if not show():
    raise SystemExit("\nNo users exist. Start the app and create the administrator.")

name = input("\nUsername to fix: ").strip()
if not con.execute("SELECT 1 FROM app_users WHERE username = ? COLLATE NOCASE", (name,)).fetchone():
    raise SystemExit("No such user.")

con.execute("UPDATE app_users SET status='active' WHERE username = ? COLLATE NOCASE", (name,))

if input("Also set a new password? (y/n): ").lower().startswith("y"):
    pw = getpass.getpass("New password (min 8 chars): ")
    if len(pw) < 8:
        raise SystemExit("Password too short.")
    salt = secrets.token_bytes(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt, ITER)
    con.execute("UPDATE app_users SET salt=?, password_hash=? WHERE username = ? COLLATE NOCASE",
                (salt, h, name))

con.commit()
print("\nDone.")
show()