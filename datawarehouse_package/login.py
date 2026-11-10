
import os
import csv
import time
import random
import sqlite3
import hashlib
import hmac
import secrets
import re
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, "datasets")
DB = os.path.join(BASE, "datawarehouse.db")
AUTH_DB = os.path.join(BASE, "auth.db")  # kept separate from the resettable warehouse database

C_BRONZE, C_SILVER, C_GOLD = "#F4B183", "#BFBFBF", "#FFD966"
C_IDLE, C_RUN, C_DONE = "#EDEDED", "#FFC000", "#92D050"


# =====================================================================
#  STEP 1 - SOURCES: validate and read existing CRM + ERP CSV files
# =====================================================================
# Default folder structure:
# datasets/
#   source_crm/cust_info.csv
#   source_crm/prd_info.csv
#   source_crm/sales_details.csv
#   source_erp/CUST_AZ12.csv
#   source_erp/LOC_A101.csv
#   source_erp/PX_CAT_G1V2.csv
#
# The app also accepts all six CSV files directly inside the selected folder.
SOURCE_HEADERS = {
    "source_crm/cust_info.csv": [
        "cst_id", "cst_key", "cst_firstname", "cst_lastname",
        "cst_marital_status", "cst_gndr", "cst_create_date"
    ],
    "source_crm/prd_info.csv": [
        "prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line",
        "prd_start_dt", "prd_end_dt"
    ],
    "source_crm/sales_details.csv": [
        "sls_ord_num", "sls_prd_key", "sls_cust_id", "sls_order_dt",
        "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price"
    ],
    "source_erp/CUST_AZ12.csv": ["CID", "BDATE", "GEN"],
    "source_erp/LOC_A101.csv": ["CID", "CNTRY"],
    "source_erp/PX_CAT_G1V2.csv": ["ID", "CAT", "SUBCAT", "MAINTENANCE"],
}

BRONZE_FILES = {
    "bronze_crm_cust_info": "source_crm/cust_info.csv",
    "bronze_crm_prd_info": "source_crm/prd_info.csv",
    "bronze_crm_sales_details": "source_crm/sales_details.csv",
    "bronze_erp_cust_az12": "source_erp/CUST_AZ12.csv",
    "bronze_erp_loc_a101": "source_erp/LOC_A101.csv",
    "bronze_erp_px_cat_g1v2": "source_erp/PX_CAT_G1V2.csv",
}


def resolve_source_file(source_dir, relative_path):
    """Find a source CSV in the standard subfolders or directly in source_dir."""
    candidates = [
        os.path.join(source_dir, relative_path),
        os.path.join(source_dir, os.path.basename(relative_path)),
    ]
    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate

    # Last attempt: locate the expected filename in a nested folder, ignoring case.
    expected_name = os.path.basename(relative_path).casefold()
    matches = []
    for current, _, filenames in os.walk(source_dir):
        for filename in filenames:
            if filename.casefold() == expected_name:
                matches.append(os.path.join(current, filename))
    if len(matches) == 1:
        return matches[0]
    return None


def validate_source_files(source_dir, log):
    """Check all six existing CSV files and return their paths and row counts."""
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(
            "The selected CSV folder does not exist:\n%s" % source_dir
        )

    found = {}
    missing = []
    problems = []

    for relative_path, expected_header in SOURCE_HEADERS.items():
        full_path = resolve_source_file(source_dir, relative_path)
        if not full_path:
            missing.append(relative_path)
            continue

        try:
            with open(full_path, "r", newline="", encoding="utf-8-sig") as csv_file:
                reader = csv.reader(csv_file)
                actual_header = next(reader, None)
                if actual_header is None:
                    problems.append("%s is empty." % relative_path)
                    continue

                actual_clean = [column.strip() for column in actual_header]
                if [c.casefold() for c in actual_clean] != [c.casefold() for c in expected_header]:
                    problems.append(
                        "%s has an unexpected header.\nExpected: %s\nFound: %s"
                        % (relative_path, ", ".join(expected_header), ", ".join(actual_clean))
                    )
                    continue

                row_count = 0
                for line_number, row in enumerate(reader, start=2):
                    if not row or all(not value.strip() for value in row):
                        continue
                    if len(row) != len(actual_header):
                        problems.append(
                            "%s has %d columns on row %d; expected %d."
                            % (relative_path, len(row), line_number, len(actual_header))
                        )
                        break
                    row_count += 1

                if not any(relative_path in problem for problem in problems):
                    found[relative_path] = {"path": full_path, "rows": row_count}
        except (OSError, csv.Error, UnicodeError) as exc:
            problems.append("Could not read %s: %s" % (relative_path, exc))

    if missing or problems:
        details = []
        if missing:
            details.append("Missing required CSV files:\n- " + "\n- ".join(missing))
        if problems:
            details.append("CSV file problems:\n- " + "\n- ".join(problems))
        details.append(
            "\nSelect the folder that contains the source_crm and source_erp folders, "
            "or a folder containing all six CSV files directly."
        )
        raise ValueError("\n\n".join(details))

    for relative_path, info in found.items():
        log("  found      %-28s %5d rows" % (relative_path, info["rows"]))
    log("  Source check complete: %d CSV files, %d total rows." % (
        len(found), sum(info["rows"] for info in found.values())
    ))
    return found


# =====================================================================
#  STEP 2 - BRONZE: raw load, no transformation (truncate & insert)
# =====================================================================
def load_bronze(con, log, source_dir):
    for table, relative_path in BRONZE_FILES.items():
        full_path = resolve_source_file(source_dir, relative_path)
        if not full_path:
            raise FileNotFoundError("Required source CSV not found: %s" % relative_path)

        with open(full_path, "r", newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.reader(csv_file)
            header = next(reader, None)
            if not header:
                raise ValueError("CSV file is empty: %s" % relative_path)
            rows = [
                row for row in reader
                if row and any(value.strip() for value in row)
            ]

        if any(len(row) != len(header) for row in rows):
            raise ValueError("CSV rows do not match the header in %s" % relative_path)

        con.execute(
            "CREATE TABLE IF NOT EXISTS %s (%s)"
            % (table, ", ".join('"' + h.replace('"', '""') + '" TEXT' for h in header))
        )
        before = con.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
        con.execute("DELETE FROM " + table)  # full refresh of the Bronze table
        placeholders = ",".join("?" * len(header))
        con.executemany("INSERT INTO %s VALUES (%s)" % (table, placeholders), rows)
        log("  %-28s truncated (%d rows) -> inserted %d rows" % (table, before, len(rows)))
    con.commit()


# =====================================================================
#  STEP 3 - SILVER: cleansing, standardization, normalization,
#           derived columns, enrichment
# =====================================================================
SILVER_SQL = {
    "silver_crm_cust_info": """
        SELECT CAST(cst_id AS INTEGER) AS cst_id, cst_key,
               TRIM(cst_firstname) AS cst_firstname,
               TRIM(cst_lastname)  AS cst_lastname,
               CASE UPPER(TRIM(cst_marital_status)) WHEN 'M' THEN 'Married' WHEN 'S' THEN 'Single' ELSE 'n/a' END AS cst_marital_status,
               CASE UPPER(TRIM(cst_gndr)) WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'n/a' END AS cst_gndr,
               cst_create_date,
               datetime('now') AS dwh_create_date
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
              FROM bronze_crm_cust_info WHERE TRIM(cst_id) <> '')
        WHERE flag_last = 1""",
    "silver_crm_prd_info": """
        SELECT CAST(b.prd_id AS INTEGER) AS prd_id,
               REPLACE(SUBSTR(b.prd_key, 1, 5), '-', '_') AS cat_id,
               SUBSTR(b.prd_key, 7) AS prd_key,
               TRIM(b.prd_nm) AS prd_nm,
               COALESCE(CAST(NULLIF(TRIM(b.prd_cost), '') AS REAL), 0) AS prd_cost,
               CASE UPPER(TRIM(b.prd_line)) WHEN 'M' THEN 'Mountain' WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales' WHEN 'T' THEN 'Touring' ELSE 'n/a' END AS prd_line,
               b.prd_start_dt,
               DATE(LEAD(b.prd_start_dt) OVER (PARTITION BY b.prd_key ORDER BY b.prd_start_dt), '-1 day') AS prd_end_dt,
               datetime('now') AS dwh_create_date
        FROM bronze_crm_prd_info b""",
    "silver_crm_sales_details": """
        SELECT sls_ord_num, sls_prd_key, sls_cust_id,
               CASE WHEN o IS NULL OR o = '0' OR LENGTH(o) <> 8 THEN NULL
                    ELSE SUBSTR(o,1,4)||'-'||SUBSTR(o,5,2)||'-'||SUBSTR(o,7,2) END AS order_dt,
               SUBSTR(s,1,4)||'-'||SUBSTR(s,5,2)||'-'||SUBSTR(s,7,2) AS ship_dt,
               SUBSTR(d,1,4)||'-'||SUBSTR(d,5,2)||'-'||SUBSTR(d,7,2) AS due_dt,
               sales_fix AS sales, qty,
               CASE WHEN price_raw IS NULL OR price_raw <= 0 THEN sales_fix / NULLIF(qty, 0) ELSE price_raw END AS price,
               datetime('now') AS dwh_create_date
        FROM (SELECT sls_ord_num, sls_prd_key, CAST(sls_cust_id AS INTEGER) AS sls_cust_id,
                     sls_order_dt AS o, sls_ship_dt AS s, sls_due_dt AS d,
                     CAST(sls_quantity AS INTEGER) AS qty,
                     CAST(NULLIF(TRIM(sls_price), '') AS REAL) AS price_raw,
                     CASE WHEN CAST(NULLIF(TRIM(sls_sales), '') AS REAL) IS NOT NULL
                               AND CAST(sls_sales AS REAL) > 0
                               AND (NULLIF(TRIM(sls_price), '') IS NULL
                                    OR ABS(CAST(sls_sales AS REAL) - CAST(sls_quantity AS INTEGER) * ABS(CAST(sls_price AS REAL))) < 0.01)
                          THEN CAST(sls_sales AS REAL)
                          ELSE CAST(sls_quantity AS INTEGER) * ABS(CAST(NULLIF(TRIM(sls_price), '') AS REAL)) END AS sales_fix
              FROM bronze_crm_sales_details)""",
    "silver_erp_cust_az12": """
        SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4) ELSE cid END AS cid,
               CASE WHEN bdate > DATE('now') THEN NULL ELSE bdate END AS bdate,
               CASE UPPER(TRIM(gen)) WHEN 'F' THEN 'Female' WHEN 'FEMALE' THEN 'Female'
                    WHEN 'M' THEN 'Male' WHEN 'MALE' THEN 'Male' ELSE 'n/a' END AS gen,
               datetime('now') AS dwh_create_date
        FROM bronze_erp_cust_az12""",
    "silver_erp_loc_a101": """
        SELECT REPLACE(cid, '-', '') AS cid,
               CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                    ELSE TRIM(cntry) END AS cntry,
               datetime('now') AS dwh_create_date
        FROM bronze_erp_loc_a101""",
    "silver_erp_px_cat_g1v2": """
        SELECT id, TRIM(cat) AS cat, TRIM(subcat) AS subcat, TRIM(maintenance) AS maintenance,
               datetime('now') AS dwh_create_date
        FROM bronze_erp_px_cat_g1v2""",
}


def build_silver(con, log):
    for table, sql in SILVER_SQL.items():
        con.execute("DROP TABLE IF EXISTS " + table)                      # full load
        con.execute("CREATE TABLE %s AS %s" % (table, sql))
        b = con.execute("SELECT COUNT(*) FROM " + table.replace("silver", "bronze")).fetchone()[0]
        s = con.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
        log("  %-28s bronze %5d rows -> silver %5d rows  (%d removed)" % (table, b, s, b - s))
    con.commit()


# =====================================================================
#  STEP 4 - GOLD: integration, business logic, star schema (views)
# =====================================================================
GOLD_SQL = {
    "gold_dim_customers": """
        SELECT ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
               ci.cst_id AS customer_id, ci.cst_key AS customer_number,
               ci.cst_firstname AS first_name, ci.cst_lastname AS last_name,
               la.cntry AS country, ci.cst_marital_status AS marital_status,
               CASE WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr ELSE COALESCE(ca.gen, 'n/a') END AS gender,
               ca.bdate AS birthdate, ci.cst_create_date AS create_date
        FROM silver_crm_cust_info ci
        LEFT JOIN silver_erp_cust_az12 ca ON ci.cst_key = ca.cid
        LEFT JOIN silver_erp_loc_a101  la ON ci.cst_key = la.cid""",
    "gold_dim_products": """
        SELECT ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
               pn.prd_id AS product_id, pn.prd_key AS product_number, pn.prd_nm AS product_name,
               pn.cat_id AS category_id, pc.cat AS category, pc.subcat AS subcategory,
               pc.maintenance, pn.prd_cost AS cost, pn.prd_line AS product_line,
               pn.prd_start_dt AS start_date
        FROM silver_crm_prd_info pn
        LEFT JOIN silver_erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
        WHERE pn.prd_end_dt IS NULL""",
    "gold_fact_sales": """
        SELECT sd.sls_ord_num AS order_number, pr.product_key, cu.customer_key,
               sd.order_dt AS order_date, sd.ship_dt AS shipping_date, sd.due_dt AS due_date,
               sd.sales AS sales_amount, sd.qty AS quantity, sd.price
        FROM silver_crm_sales_details sd
        LEFT JOIN gold_dim_products  pr ON sd.sls_prd_key = pr.product_number
        LEFT JOIN gold_dim_customers cu ON sd.sls_cust_id = cu.customer_id""",
    "gold_flat_sales": """
        SELECT f.order_number, f.order_date, f.sales_amount, f.quantity, f.price,
               c.first_name || ' ' || c.last_name AS customer, c.country, c.gender, c.marital_status,
               p.product_name, p.category, p.subcategory, p.product_line
        FROM gold_fact_sales f
        LEFT JOIN gold_dim_customers c ON f.customer_key = c.customer_key
        LEFT JOIN gold_dim_products  p ON f.product_key  = p.product_key""",
    "gold_agg_monthly_sales": """
        SELECT SUBSTR(order_date, 1, 7) AS month, COUNT(DISTINCT order_number) AS orders,
               SUM(quantity) AS units, ROUND(SUM(sales_amount), 2) AS total_sales
        FROM gold_fact_sales WHERE order_date IS NOT NULL
        GROUP BY SUBSTR(order_date, 1, 7)""",
}


def build_gold(con, log):
    for view, sql in GOLD_SQL.items():
        con.execute("DROP VIEW IF EXISTS " + view)                        # no load: only views
        con.execute("CREATE VIEW %s AS %s" % (view, sql))
        n = con.execute("SELECT COUNT(*) FROM " + view).fetchone()[0]
        log("  %-28s view created (%d rows visible)" % (view, n))
    con.commit()


# =====================================================================
#  DATA QUALITY CHECKS  (bronze  vs  silver)
# =====================================================================
QUALITY = [
    ("Duplicate customer IDs",
     "SELECT COUNT(*) FROM (SELECT cst_id FROM bronze_crm_cust_info WHERE TRIM(cst_id)<>'' GROUP BY cst_id HAVING COUNT(*)>1)",
     "SELECT COUNT(*) FROM (SELECT cst_id FROM silver_crm_cust_info GROUP BY cst_id HAVING COUNT(*)>1)"),
    ("Missing customer IDs",
     "SELECT COUNT(*) FROM bronze_crm_cust_info WHERE TRIM(cst_id)=''",
     "SELECT COUNT(*) FROM silver_crm_cust_info WHERE cst_id IS NULL"),
    ("Names with unwanted spaces",
     "SELECT COUNT(*) FROM bronze_crm_cust_info WHERE cst_firstname<>TRIM(cst_firstname) OR cst_lastname<>TRIM(cst_lastname)",
     "SELECT COUNT(*) FROM silver_crm_cust_info WHERE cst_firstname<>TRIM(cst_firstname) OR cst_lastname<>TRIM(cst_lastname)"),
    ("Gender / marital codes not standardized (M, F, S, blank)",
     "SELECT COUNT(*) FROM bronze_crm_cust_info WHERE cst_gndr IN ('M','F','') OR cst_marital_status IN ('M','S','')",
     "SELECT COUNT(*) FROM silver_crm_cust_info WHERE cst_gndr NOT IN ('Male','Female','n/a') OR cst_marital_status NOT IN ('Married','Single','n/a')"),
    ("ERP country codes not standardized (DE, US, USA, blank)",
     "SELECT COUNT(*) FROM bronze_erp_loc_a101 WHERE cntry IN ('DE','US','USA','')",
     "SELECT COUNT(*) FROM silver_erp_loc_a101 WHERE cntry IN ('DE','US','USA','')"),
    ("ERP customer id with 'NAS' prefix",
     "SELECT COUNT(*) FROM bronze_erp_cust_az12 WHERE cid LIKE 'NAS%'",
     "SELECT COUNT(*) FROM silver_erp_cust_az12 WHERE cid LIKE 'NAS%'"),
    ("Birth dates in the future",
     "SELECT COUNT(*) FROM bronze_erp_cust_az12 WHERE bdate > DATE('now')",
     "SELECT COUNT(*) FROM silver_erp_cust_az12 WHERE bdate > DATE('now')"),
    ("Products without cost",
     "SELECT COUNT(*) FROM bronze_crm_prd_info WHERE TRIM(prd_cost)=''",
     "SELECT COUNT(*) FROM silver_crm_prd_info WHERE prd_cost IS NULL"),
    ("Product versions without end date (history not closed)",
     "SELECT COUNT(*) FROM bronze_crm_prd_info WHERE TRIM(prd_end_dt)=''",
     "SELECT COUNT(*) FROM silver_crm_prd_info s WHERE prd_end_dt IS NULL AND EXISTS (SELECT 1 FROM silver_crm_prd_info t WHERE t.prd_key=s.prd_key AND t.prd_start_dt>s.prd_start_dt)"),
    ("Invalid order dates",
     "SELECT COUNT(*) FROM bronze_crm_sales_details WHERE sls_order_dt='0' OR LENGTH(sls_order_dt)<>8",
     "SELECT COUNT(*) FROM silver_crm_sales_details WHERE order_dt IS NOT NULL AND order_dt NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'"),
    ("Sales <> quantity x price",
     "SELECT COUNT(*) FROM bronze_crm_sales_details WHERE TRIM(sls_price)<>'' AND ABS(CAST(sls_sales AS REAL)-CAST(sls_quantity AS INTEGER)*ABS(CAST(sls_price AS REAL)))>0.01",
     "SELECT COUNT(*) FROM silver_crm_sales_details WHERE price IS NOT NULL AND ABS(sales-qty*price)>0.01"),
    ("Missing or negative price",
     "SELECT COUNT(*) FROM bronze_crm_sales_details WHERE TRIM(sls_price)='' OR CAST(sls_price AS REAL)<=0",
     "SELECT COUNT(*) FROM silver_crm_sales_details WHERE price IS NULL OR price<=0"),
]


# =====================================================================
#  SMALL DRAWING / TABLE HELPERS (used by many tabs)
# =====================================================================
def show(tree, cur):
    """Fill a ttk.Treeview with the result of a cursor."""
    tree.delete(*tree.get_children())
    cols = [d[0] for d in cur.description]
    tree["columns"] = cols
    for c in cols:
        tree.heading(c, text=c)
        tree.column(c, width=max(90, min(200, len(c) * 11)), anchor="w")
    for row in cur.fetchall():
        tree.insert("", "end", values=["" if v is None else v for v in row])


def make_tree(parent, height=12):
    frame = ttk.Frame(parent)
    tree = ttk.Treeview(frame, show="headings", height=height)
    sy = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
    sx = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
    tree.grid(row=0, column=0, sticky="nsew")
    sy.grid(row=0, column=1, sticky="ns")
    sx.grid(row=1, column=0, sticky="ew")
    frame.rowconfigure(0, weight=1)
    frame.columnconfigure(0, weight=1)
    return frame, tree


def draw_bars(cv, title, labels, values, color):
    cv.delete("all")
    cv.create_text(10, 12, text=title, anchor="w", font=("Segoe UI", 11, "bold"))
    if not values:
        return
    top = max(values)
    for i, (lab, val) in enumerate(zip(labels, values)):
        y = 40 + i * 30
        w = 300 * val / top if top else 0
        cv.create_text(150, y + 10, text=str(lab)[:22], anchor="e", font=("Segoe UI", 9))
        cv.create_rectangle(160, y, 160 + w, y + 20, fill=color, outline="")
        cv.create_text(166 + w, y + 10, text="{:,.0f}".format(val), anchor="w", font=("Segoe UI", 9))



# =====================================================================
#  AUTHENTICATION: local user accounts, password hashing and approval
# =====================================================================
# =====================================================================
#  AUTHENTICATION: local user accounts, password hashing and approval
# =====================================================================
class AuthStore:
    """Stores local accounts in a separate database that warehouse reset cannot erase."""

    ITERATIONS = 310_000
    REQUIRE_APPROVAL = True      # False = new sign-ups can log in immediately
    MAX_FAILS = 5                # failed logins before a temporary lock
    LOCK_SECONDS = 30

    def __init__(self, path=AUTH_DB):
        self.con = sqlite3.connect(path)
        self.con.execute("PRAGMA foreign_keys = ON")
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS app_users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE COLLATE NOCASE,
                salt BLOB NOT NULL,
                password_hash BLOB NOT NULL,
                role TEXT NOT NULL DEFAULT 'user' CHECK(role IN ('admin', 'user')),
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'active', 'disabled')),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.con.commit()
        self._fails = {}   # username(lower) -> (count, locked_until)

    @staticmethod
    def validate_username(username):
        username = username.strip()
        if not re.fullmatch(r"[A-Za-z0-9_.-]{3,32}", username):
            return False, "Username must be 3-32 characters and contain only letters, numbers, dot, underscore or hyphen."
        return True, username

    @staticmethod
    def _password_hash(password, salt):
        return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, AuthStore.ITERATIONS)

    def has_admin(self):
        row = self.con.execute("SELECT COUNT(*) FROM app_users WHERE role = 'admin'").fetchone()
        return row[0] > 0

    def _insert_user(self, username, password, role, status):
        ok, result = self.validate_username(username)
        if not ok:
            return False, result
        username = result
        if len(password) < 8:
            return False, "Password must contain at least 8 characters."

        salt = secrets.token_bytes(16)
        password_hash = self._password_hash(password, salt)
        try:
            self.con.execute(
                "INSERT INTO app_users (username, salt, password_hash, role, status) VALUES (?, ?, ?, ?, ?)",
                (username, salt, password_hash, role, status)
            )
            self.con.commit()
            return True, "Account created successfully."
        except sqlite3.IntegrityError:
            return False, "That username is already registered. Please choose another username."

    def create_first_admin(self, username, password):
        if self.has_admin():
            return False, "An administrator account already exists."
        return self._insert_user(username, password, "admin", "active")

    def signup(self, username, password):
        # Public sign-up never grants administrator privileges.
        status = "pending" if self.REQUIRE_APPROVAL else "active"
        ok, msg = self._insert_user(username, password, "user", status)
        if ok:
            msg = ("Your account has been created and is waiting for administrator approval."
                   if status == "pending" else
                   "Your account has been created. You can log in now.")
        return ok, msg

    def authenticate(self, username, password):
        key = username.strip().casefold()
        if not key or not password:
            return None, "Please enter both username and password."

        count, locked_until = self._fails.get(key, (0, 0))
        if locked_until > time.time():
            wait = int(locked_until - time.time()) + 1
            return None, "Too many failed attempts. Try again in %d seconds." % wait

        row = self.con.execute(
            "SELECT username, salt, password_hash, role, status "
            "FROM app_users WHERE username = ? COLLATE NOCASE",
            (username.strip(),)
        ).fetchone()

        if row is None:
            # Do the same amount of work so timing does not reveal valid usernames.
            self._password_hash(password, b"\x00" * 16)
            self._register_fail(key)
            return None, "Invalid username or password."

        saved_username, salt, saved_hash, role, status = row
        entered_hash = self._password_hash(password, salt)
        if not hmac.compare_digest(entered_hash, saved_hash):
            self._register_fail(key)
            return None, "Invalid username or password."

        self._fails.pop(key, None)
        if status == "pending":
            return None, "Your account is waiting for administrator approval."
        if status == "disabled":
            return None, "Your account has been disabled. Contact the administrator."
        return {"username": saved_username, "role": role}, None

    def _register_fail(self, key):
        count, _ = self._fails.get(key, (0, 0))
        count += 1
        if count >= self.MAX_FAILS:
            self._fails[key] = (0, time.time() + self.LOCK_SECONDS)
        else:
            self._fails[key] = (count, 0)

    def list_users(self):
        return self.con.execute(
            "SELECT username, role, status, created_at FROM app_users ORDER BY created_at DESC"
        ).fetchall()

    def set_status(self, username, status):
        if status not in ("active", "disabled"):
            raise ValueError("Invalid account status")
        self.con.execute(
            "UPDATE app_users SET status = ? WHERE username = ? COLLATE NOCASE",
            (status, username)
        )
        self.con.commit()

    def close(self):
        try:
            self.con.close()
        except sqlite3.Error:
            pass


class AuthGate:
    """Shows login/signup before the data warehouse application can be opened."""

    BG = "#F3F6FA"

    def __init__(self, root, auth_store=None):
        self.root = root
        self.auth = auth_store or AuthStore()
        self.root.title("Data Warehouse - Secure Login")
        self.root.configure(bg=self.BG)
        self.root.resizable(True, True)
        self.root.protocol("WM_DELETE_WINDOW", self.close_app)

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background=self.BG)
        style.configure("TLabel", background=self.BG)
        style.configure("Card.TFrame", background="#FFFFFF")
        style.configure("Card.TLabel", background="#FFFFFF", font=("Segoe UI", 10))
        style.configure("Card.TCheckbutton", background="#FFFFFF", font=("Segoe UI", 9))
        style.configure("Auth.TButton", font=("Segoe UI", 10, "bold"), padding=8)
        style.configure("AuthTitle.TLabel", font=("Segoe UI", 19, "bold"), foreground="#1F3864", background=self.BG)
        style.configure("AuthSub.TLabel", font=("Segoe UI", 10), foreground="#64748B", background=self.BG)
        style.configure("AuthHead.TLabel", font=("Segoe UI", 14, "bold"), background=self.BG)

        if not self.auth.has_admin():
            self.show_admin_setup()
        else:
            self.show_login()

    # ---------------------------------------------------------- helpers
    def clear(self):
        for widget in self.root.winfo_children():
            widget.destroy()

    def fit_window(self):
        """Size the window to its content so no button is ever cut off (any DPI scaling)."""
        self.root.update_idletasks()
        w = max(480, self.root.winfo_reqwidth())
        h = self.root.winfo_reqheight()
        x = max(0, (self.root.winfo_screenwidth() - w) // 2)
        y = max(0, (self.root.winfo_screenheight() - h) // 3)
        self.root.minsize(w, h)
        self.root.geometry("%dx%d+%d+%d" % (w, h, x, y))

    def make_page(self, title, subtitle):
        self.clear()
        outer = ttk.Frame(self.root, padding=28)
        outer.pack(expand=True, fill="both")
        ttk.Label(outer, text="DATA WAREHOUSE", style="AuthTitle.TLabel").pack(pady=(6, 4))
        ttk.Label(outer, text=title, style="AuthHead.TLabel").pack(pady=(10, 4))
        ttk.Label(outer, text=subtitle, style="AuthSub.TLabel", wraplength=380,
                  justify="center").pack(pady=(0, 14))
        card = ttk.Frame(outer, padding=20, relief="ridge", style="Card.TFrame")
        card.pack(fill="x", padx=10)
        return card, outer

    def add_field(self, card, label, secret=False, last=False):
        ttk.Label(card, text=label, style="Card.TLabel").pack(anchor="w")
        entry = ttk.Entry(card, width=38, show="*" if secret else "")
        entry.pack(fill="x", pady=(4, 14 if last else 10))
        return entry

    def add_show_password(self, card, *entries):
        var = tk.BooleanVar(value=False)

        def toggle():
            for e in entries:
                e.config(show="" if var.get() else "*")

        ttk.Checkbutton(card, text="Show password", variable=var, command=toggle,
                        style="Card.TCheckbutton").pack(anchor="w", pady=(0, 12))

    # ---------------------------------------------------------- first admin
    def show_admin_setup(self):
        card, _ = self.make_page(
            "First-time administrator setup",
            "Create the first administrator account. This account can approve or disable other users."
        )
        self.admin_user = self.add_field(card, "Administrator username")
        self.admin_pass = self.add_field(card, "Password (minimum 8 characters)", secret=True)
        self.admin_confirm = self.add_field(card, "Confirm password", secret=True)
        self.add_show_password(card, self.admin_pass, self.admin_confirm)
        ttk.Button(card, text="Create Administrator", style="Auth.TButton",
                   command=self.create_admin).pack(fill="x")
        for e in (self.admin_user, self.admin_pass, self.admin_confirm):
            e.bind("<Return>", lambda event: self.create_admin())
        self.admin_user.focus_set()
        self.fit_window()

    def create_admin(self):
        password = self.admin_pass.get()
        if password != self.admin_confirm.get():
            messagebox.showerror("Password mismatch", "The passwords do not match.")
            return
        ok, message = self.auth.create_first_admin(self.admin_user.get(), password)
        if not ok:
            messagebox.showerror("Administrator setup", message)
            return
        messagebox.showinfo("Administrator created", "Administrator account created. Please log in.")
        self.show_login()

    # ---------------------------------------------------------- login
    def show_login(self):
        sub = ("Only active, administrator-approved accounts can access the Data Warehouse."
               if AuthStore.REQUIRE_APPROVAL else "Sign in to open the Data Warehouse.")
        card, outer = self.make_page("Sign in to your account", sub)
        self.login_user = self.add_field(card, "Username")
        self.login_pass = self.add_field(card, "Password", secret=True)
        self.add_show_password(card, self.login_pass)
        ttk.Button(card, text="LOGIN", style="Auth.TButton", command=self.login).pack(fill="x")
        ttk.Button(outer, text="Create a new account / Sign Up", command=self.show_signup).pack(pady=16)
        for e in (self.login_user, self.login_pass):
            e.bind("<Return>", lambda event: self.login())
        self.login_user.focus_set()
        self.fit_window()

    def login(self):
        user, error = self.auth.authenticate(self.login_user.get(), self.login_pass.get())
        if error:
            messagebox.showwarning("Login unsuccessful", error)
            self.login_pass.delete(0, "end")
            self.login_pass.focus_set()
            return
        self.launch_application(user)

    # ---------------------------------------------------------- sign up
    def show_signup(self):
        sub = ("New accounts remain pending until an administrator approves them."
               if AuthStore.REQUIRE_APPROVAL else "Create an account to use the Data Warehouse.")
        card, outer = self.make_page("Create an account", sub)
        self.signup_user = self.add_field(card, "Choose a username")
        self.signup_pass = self.add_field(card, "Password (minimum 8 characters)", secret=True)
        self.signup_confirm = self.add_field(card, "Confirm password", secret=True)
        self.add_show_password(card, self.signup_pass, self.signup_confirm)
        ttk.Button(card, text="SIGN UP", style="Auth.TButton", command=self.signup).pack(fill="x")
        ttk.Button(outer, text="Back to Login", command=self.show_login).pack(pady=14)
        for e in (self.signup_user, self.signup_pass, self.signup_confirm):
            e.bind("<Return>", lambda event: self.signup())
        self.signup_user.focus_set()
        self.fit_window()

    def signup(self):
        password = self.signup_pass.get()
        if password != self.signup_confirm.get():
            messagebox.showerror("Password mismatch", "The passwords do not match.")
            return
        ok, message = self.auth.signup(self.signup_user.get(), password)
        if not ok:
            messagebox.showerror("Sign up", message)
            return
        messagebox.showinfo("Registration submitted", message)
        self.show_login()

    # ---------------------------------------------------------- launch / close
    def launch_application(self, user):
        self.clear()
        self.root.minsize(1000, 650)
        self.root.geometry("1280x820+20+20")
        self.root.title("Data Warehouse Demo - Bronze / Silver / Gold")
        self.app = App(self.root, user, self.auth)

    def close_app(self):
        self.auth.close()
        self.root.destroy()


# class AuthStore:
#     """Stores local accounts in a separate database that warehouse reset cannot erase."""

#     ITERATIONS = 310_000

#     def __init__(self, path=AUTH_DB):
#         self.con = sqlite3.connect(path)
#         self.con.execute("PRAGMA foreign_keys = ON")
#         self.con.execute("""
#             CREATE TABLE IF NOT EXISTS app_users (
#                 user_id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 username TEXT NOT NULL UNIQUE COLLATE NOCASE,
#                 salt BLOB NOT NULL,
#                 password_hash BLOB NOT NULL,
#                 role TEXT NOT NULL DEFAULT 'user' CHECK(role IN ('admin', 'user')),
#                 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'active', 'disabled')),
#                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
#             )
#         """)
#         self.con.commit()

#     @staticmethod
#     def validate_username(username):
#         username = username.strip()
#         if not re.fullmatch(r"[A-Za-z0-9_.-]{3,32}", username):
#             return False, "Username must be 3–32 characters and contain only letters, numbers, dot, underscore or hyphen."
#         return True, username

#     @staticmethod
#     def _password_hash(password, salt):
#         return hashlib.pbkdf2_hmac(
#             "sha256", password.encode("utf-8"), salt, AuthStore.ITERATIONS
#         )

#     def has_admin(self):
#         row = self.con.execute(
#             "SELECT COUNT(*) FROM app_users WHERE role = 'admin'"
#         ).fetchone()
#         return row[0] > 0

#     def _insert_user(self, username, password, role, status):
#         ok, result = self.validate_username(username)
#         if not ok:
#             return False, result
#         username = result
#         if len(password) < 8:
#             return False, "Password must contain at least 8 characters."

#         salt = secrets.token_bytes(16)
#         password_hash = self._password_hash(password, salt)
#         try:
#             self.con.execute(
#                 "INSERT INTO app_users (username, salt, password_hash, role, status) VALUES (?, ?, ?, ?, ?)",
#                 (username, salt, password_hash, role, status)
#             )
#             self.con.commit()
#             return True, "Account created successfully."
#         except sqlite3.IntegrityError:
#             return False, "That username is already registered. Please choose another username."

#     def create_first_admin(self, username, password):
#         if self.has_admin():
#             return False, "An administrator account already exists."
#         return self._insert_user(username, password, "admin", "active")

#     def signup(self, username, password):
#         # Public sign-up never grants administrator privileges or immediate access.
#         return self._insert_user(username, password, "user", "pending")

#     def authenticate(self, username, password):
#         row = self.con.execute(
#             "SELECT username, salt, password_hash, role, status "
#             "FROM app_users WHERE username = ? COLLATE NOCASE",
#             (username.strip(),)
#         ).fetchone()
#         if row is None:
#             return None, "Invalid username or password."

#         saved_username, salt, saved_hash, role, status = row
#         entered_hash = self._password_hash(password, salt)
#         if not hmac.compare_digest(entered_hash, saved_hash):
#             return None, "Invalid username or password."
#         if status == "pending":
#             return None, "Your account is waiting for administrator approval."
#         if status == "disabled":
#             return None, "Your account has been disabled. Contact the administrator."

#         return {"username": saved_username, "role": role}, None

#     def list_users(self):
#         return self.con.execute(
#             "SELECT username, role, status, created_at FROM app_users ORDER BY created_at DESC"
#         ).fetchall()

#     def set_status(self, username, status):
#         if status not in ("active", "disabled"):
#             raise ValueError("Invalid account status")
#         self.con.execute(
#             "UPDATE app_users SET status = ? WHERE username = ? COLLATE NOCASE",
#             (status, username)
#         )
#         self.con.commit()

#     def close(self):
#         try:
#             self.con.close()
#         except sqlite3.Error:
#             pass


# class AuthGate:
#     """Shows login/signup before the data warehouse application can be opened."""

#     def __init__(self, root, auth_store=None):
#         self.root = root
#         self.auth = auth_store or AuthStore()
#         self.root.title("Data Warehouse - Secure Login")
#         self.root.geometry("480x540")
#         self.root.minsize(430, 470)
#         self.root.configure(bg="#F3F6FA")
#         self.root.protocol("WM_DELETE_WINDOW", self.close_app)

#         style = ttk.Style()
#         style.theme_use("clam")
#         style.configure("Auth.TButton", font=("Segoe UI", 10, "bold"), padding=8)
#         style.configure("AuthTitle.TLabel", font=("Segoe UI", 19, "bold"), foreground="#1F3864")
#         style.configure("AuthSub.TLabel", font=("Segoe UI", 10), foreground="#64748B")

#         if not self.auth.has_admin():
#             self.show_admin_setup()
#         else:
#             self.show_login()

#     def clear(self):
#         for widget in self.root.winfo_children():
#             widget.destroy()

#     def make_page(self, title, subtitle):
#         self.clear()
#         outer = ttk.Frame(self.root, padding=28)
#         outer.pack(expand=True, fill="both")
#         ttk.Label(outer, text="DATA WAREHOUSE", style="AuthTitle.TLabel").pack(pady=(10, 4))
#         ttk.Label(outer, text=title, font=("Segoe UI", 14, "bold")).pack(pady=(14, 4))
#         ttk.Label(outer, text=subtitle, style="AuthSub.TLabel", wraplength=380).pack(pady=(0, 18))
#         card = ttk.Frame(outer, padding=20, relief="ridge")
#         card.pack(fill="x", padx=10)
#         return card, outer

#     def show_admin_setup(self):
#         card, _ = self.make_page(
#             "First-time administrator setup",
#             "Create the first administrator account. This account can approve or disable other users."
#         )
#         ttk.Label(card, text="Administrator username").pack(anchor="w")
#         self.admin_user = ttk.Entry(card, width=38)
#         self.admin_user.pack(fill="x", pady=(4, 12))
#         ttk.Label(card, text="Password (minimum 8 characters)").pack(anchor="w")
#         self.admin_pass = ttk.Entry(card, show="*", width=38)
#         self.admin_pass.pack(fill="x", pady=(4, 12))
#         ttk.Label(card, text="Confirm password").pack(anchor="w")
#         self.admin_confirm = ttk.Entry(card, show="*", width=38)
#         self.admin_confirm.pack(fill="x", pady=(4, 16))
#         ttk.Button(card, text="Create Administrator", style="Auth.TButton",
#                    command=self.create_admin).pack(fill="x")
#         ttk.Label(card, text="The first account is the administrator. Later sign-ups require approval.",
#                   wraplength=340, foreground="#64748B").pack(pady=(14, 0))

#     def create_admin(self):
#         password = self.admin_pass.get()
#         if password != self.admin_confirm.get():
#             messagebox.showerror("Password mismatch", "The passwords do not match.")
#             return
#         ok, message = self.auth.create_first_admin(self.admin_user.get(), password)
#         if not ok:
#             messagebox.showerror("Administrator setup", message)
#             return
#         messagebox.showinfo("Administrator created", "Administrator account created. Please log in.")
#         self.show_login()

#     def show_login(self):
#         card, outer = self.make_page(
#             "Sign in to your account",
#             "Only active, administrator-approved accounts can access the Data Warehouse."
#         )
#         ttk.Label(card, text="Username").pack(anchor="w")
#         self.login_user = ttk.Entry(card, width=38)
#         self.login_user.pack(fill="x", pady=(4, 12))
#         ttk.Label(card, text="Password").pack(anchor="w")
#         self.login_pass = ttk.Entry(card, show="*", width=38)
#         self.login_pass.pack(fill="x", pady=(4, 16))
#         ttk.Button(card, text="LOGIN", style="Auth.TButton", command=self.login).pack(fill="x")
#         ttk.Button(outer, text="Create a new account / Sign Up", command=self.show_signup).pack(pady=16)
#         self.login_pass.bind("<Return>", lambda event: self.login())
#         self.login_user.focus_set()

#     def show_signup(self):
#         card, outer = self.make_page(
#             "Create an account",
#             "New accounts remain pending until an administrator approves them."
#         )
#         ttk.Label(card, text="Choose a username").pack(anchor="w")
#         self.signup_user = ttk.Entry(card, width=38)
#         self.signup_user.pack(fill="x", pady=(4, 10))
#         ttk.Label(card, text="Password (minimum 8 characters)").pack(anchor="w")
#         self.signup_pass = ttk.Entry(card, show="*", width=38)
#         self.signup_pass.pack(fill="x", pady=(4, 10))
#         ttk.Label(card, text="Confirm password").pack(anchor="w")
#         self.signup_confirm = ttk.Entry(card, show="*", width=38)
#         self.signup_confirm.pack(fill="x", pady=(4, 16))
#         ttk.Button(card, text="SIGN UP", style="Auth.TButton", command=self.signup).pack(fill="x")
#         ttk.Button(outer, text="Back to Login", command=self.show_login).pack(pady=14)

#     def signup(self):
#         password = self.signup_pass.get()
#         if password != self.signup_confirm.get():
#             messagebox.showerror("Password mismatch", "The passwords do not match.")
#             return
#         ok, message = self.auth.signup(self.signup_user.get(), password)
#         if not ok:
#             messagebox.showerror("Sign up", message)
#             return
#         messagebox.showinfo(
#             "Registration submitted",
#             "Your account has been created and is waiting for administrator approval."
#         )
#         self.show_login()

#     def login(self):
#         user, error = self.auth.authenticate(self.login_user.get(), self.login_pass.get())
#         if error:
#             messagebox.showwarning("Login unsuccessful", error)
#             return
#         self.launch_application(user)

#     def launch_application(self, user):
#         self.clear()
#         self.root.geometry("1280x820")
#         self.root.minsize(1000, 650)
#         self.root.title("Data Warehouse Demo - Bronze / Silver / Gold")
#         self.app = App(self.root, user, self.auth)

#     def close_app(self):
#         self.auth.close()
#         self.root.destroy()


# =====================================================================
#  USER INTERFACE
# =====================================================================

class App:
    def __init__(self, root, current_user, auth_store):
        self.root = root
        self.current_user = current_user
        self.auth_store = auth_store
        self.source_dir = SRC
        root.title("Data Warehouse Demo  -  Bronze / Silver / Gold")
        root.geometry("1280x820")
        root.protocol("WM_DELETE_WINDOW", self.close_application)
        # Keep the warehouse database between logins. Use the Reset warehouse button when a fresh run is needed.
        self.con = sqlite3.connect(DB)
        self.done = {"Sources": False, "Bronze": False, "Silver": False, "Gold": False}
        self.stage_ids = {}

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Big.TButton", font=("Segoe UI", 10, "bold"), padding=6)

        head = tk.Frame(root, bg="#1F3864")
        head.pack(fill="x")
        tk.Label(head, text="DATA WAREHOUSE  -  End-to-End Demonstration", bg="#1F3864", fg="white",
                 font=("Segoe UI", 16, "bold")).pack(side="left", padx=14, pady=8)
        account_bar = tk.Frame(head, bg="#1F3864")
        account_bar.pack(side="right", padx=10)
        ttk.Button(account_bar, text="Logout", command=self.logout).pack(side="right", padx=4)
        if self.current_user["role"] == "admin":
            ttk.Button(account_bar, text="Manage Users", command=self.manage_users).pack(side="right", padx=4)
        tk.Label(account_bar, text="Signed in: %s (%s)" % (self.current_user["username"], self.current_user["role"]),
                 bg="#1F3864", fg="#FFFFFF", font=("Segoe UI", 9, "bold")).pack(side="right", padx=8)

        self.nb = ttk.Notebook(root)
        self.nb.pack(fill="both", expand=True, padx=6, pady=6)
        self.tab_pipeline()
        self.tab_explorer()
        self.tab_compare()
        self.tab_quality()
        self.tab_model()
        self.tab_consume()
        self.tab_characteristics()

    # ------------------------------------------------------------ application shutdown
    def close_application(self):
        try:
            self.con.close()
        except sqlite3.Error:
            pass
        self.auth_store.close()
        self.root.destroy()

    # ------------------------------------------------------------ authentication controls
    def logout(self):
        if not messagebox.askyesno("Logout", "Are you sure you want to log out?"):
            return
        try:
            self.con.close()
        except sqlite3.Error:
            pass
        for widget in self.root.winfo_children():
            widget.destroy()
        AuthGate(self.root, self.auth_store)

    def manage_users(self):
        if self.current_user.get("role") != "admin":
            messagebox.showerror("Access denied", "Only administrators can manage user accounts.")
            return

        win = tk.Toplevel(self.root)
        win.title("User Account Management")
        win.geometry("720x430")
        win.transient(self.root)

        ttk.Label(win, text="Registered users", font=("Segoe UI", 13, "bold")).pack(anchor="w", padx=12, pady=10)
        columns = ("username", "role", "status", "created")
        tree = ttk.Treeview(win, columns=columns, show="headings", height=13)
        for col, label, width in (
            ("username", "USERNAME", 170), ("role", "ROLE", 100),
            ("status", "STATUS", 110), ("created", "CREATED AT", 220)
        ):
            tree.heading(col, text=label)
            tree.column(col, width=width, anchor="w")
        tree.pack(fill="both", expand=True, padx=12, pady=4)

        def refresh_users():
            tree.delete(*tree.get_children())
            for row in self.auth_store.list_users():
                tree.insert("", "end", values=row)

        def update_selected(status):
            selected = tree.selection()
            if not selected:
                messagebox.showinfo("Select a user", "Please select a user first.", parent=win)
                return
            username = tree.item(selected[0], "values")[0]
            if username.casefold() == self.current_user["username"].casefold() and status == "disabled":
                messagebox.showwarning("Not allowed", "You cannot disable your own administrator account.", parent=win)
                return
            self.auth_store.set_status(username, status)
            refresh_users()
            messagebox.showinfo("Account updated", "%s is now %s." % (username, status), parent=win)

        buttons = ttk.Frame(win)
        buttons.pack(fill="x", padx=12, pady=10)
        ttk.Button(buttons, text="Approve / Activate", command=lambda: update_selected("active")).pack(side="left", padx=4)
        ttk.Button(buttons, text="Disable account", command=lambda: update_selected("disabled")).pack(side="left", padx=4)
        ttk.Button(buttons, text="Refresh", command=refresh_users).pack(side="right", padx=4)
        refresh_users()

    # ------------------------------------------------------------ log
    def log(self, msg):
        self.logbox.insert("end", "[%s] %s\n" % (time.strftime("%H:%M:%S"), msg))
        self.logbox.see("end")
        self.root.update()

    # ------------------------------------------------------------ pipeline tab
    def tab_pipeline(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="1. Pipeline (ETL)")

        bar = ttk.Frame(tab)
        bar.pack(fill="x", padx=8, pady=8)
        ttk.Button(bar, text="1  Read Source CSVs", style="Big.TButton", command=self.run_sources).pack(side="left", padx=3)
        ttk.Button(bar, text="2  Load Bronze", style="Big.TButton", command=self.run_bronze).pack(side="left", padx=3)
        ttk.Button(bar, text="3  Build Silver", style="Big.TButton", command=self.run_silver).pack(side="left", padx=3)
        ttk.Button(bar, text="4  Build Gold", style="Big.TButton", command=self.run_gold).pack(side="left", padx=3)
        ttk.Button(bar, text="RUN FULL PIPELINE", style="Big.TButton", command=self.run_all).pack(side="left", padx=18)
        ttk.Button(bar, text="Reset warehouse", command=self.reset).pack(side="right", padx=3)
        ttk.Button(bar, text="Select CSV Folder", command=self.browse_source_folder).pack(side="right", padx=8)

        self.source_path_label = ttk.Label(
            tab, text="CSV folder: %s" % self.source_dir, foreground="#444"
        )
        self.source_path_label.pack(anchor="w", padx=10, pady=(0, 4))

        cv = tk.Canvas(tab, width=1230, height=260, bg="white", highlightthickness=1, highlightbackground="#999")
        cv.pack(padx=8)
        self.pcv = cv
        info = [
            ("Sources", "#D9E1F2", ["CRM: 3 existing CSV files", "ERP: 3 existing CSV files", "Read-only source files"]),
            ("Bronze Layer", C_BRONZE, ["Raw data, as-is", "Batch, full load", "(truncate & insert)", "No transformations"]),
            ("Silver Layer", C_SILVER, ["Cleansing, standardization", "Normalization", "Derived columns, enrichment", "Batch, full load"]),
            ("Gold Layer", C_GOLD, ["Business-ready data", "Integration, aggregation", "Business logic", "Star schema (views)"]),
            ("Consume", "#E2EFDA", ["BI & reporting", "Ad-hoc SQL queries", "Machine learning"]),
        ]
        for i, (name, col, lines) in enumerate(info):
            x = 15 + i * 245
            rect = cv.create_rectangle(x, 20, x + 215, 210, fill=C_IDLE, outline="#444", width=2)
            cv.create_rectangle(x, 20, x + 215, 52, fill=col, outline="#444", width=2)
            cv.create_text(x + 107, 36, text=name, font=("Segoe UI", 11, "bold"))
            for j, line in enumerate(lines):
                cv.create_text(x + 107, 75 + j * 20, text=line, font=("Segoe UI", 9))
            cnt = cv.create_text(x + 107, 195, text="waiting", font=("Segoe UI", 9, "bold"), fill="#555")
            self.stage_ids[name.split()[0]] = (rect, cnt)
            if i < 4:
                cv.create_line(x + 217, 115, x + 243, 115, arrow="last", width=3, fill="#2E75B6")
        cv.create_text(615, 238, text="Grey = waiting   |   Orange = running   |   Green = completed",
                       font=("Segoe UI", 9), fill="#555")

        self.progress = ttk.Progressbar(tab, mode="determinate", maximum=100)
        self.progress.pack(fill="x", padx=8, pady=(8, 2))
        self.logbox = tk.Text(tab, height=14, bg="#0B1D33", fg="#D6F5D6", font=("Consolas", 10))
        self.logbox.pack(fill="both", expand=True, padx=8, pady=4)
        self.log("Ready. Select your CSV folder, then press 'RUN FULL PIPELINE'.")
        self.log("Expected: 3 CRM CSVs + 3 ERP CSVs. Source files will not be changed.")

    def set_stage(self, name, state, text=None):
        rect, cnt = self.stage_ids[name]
        self.pcv.itemconfig(rect, fill={"idle": C_IDLE, "run": C_RUN, "done": C_DONE}[state])
        if text:
            self.pcv.itemconfig(cnt, text=text)
        self.root.update()

    def count_tables(self, prefix):
        names = [r[0] for r in self.con.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE ? AND type IN ('table','view')", (prefix + "%",))]
        return len(names), sum(self.con.execute("SELECT COUNT(*) FROM " + n).fetchone()[0] for n in names)

    def need(self, step):
        if not self.done[step]:
            messagebox.showwarning("Order", "Please run the step before first: " + step)
            return False
        return True

    def browse_source_folder(self):
        selected = filedialog.askdirectory(
            title="Select the folder containing the six source CSV files",
            initialdir=self.source_dir if os.path.isdir(self.source_dir) else BASE,
        )
        if selected:
            self.source_dir = selected
            self.source_path_label.config(text="CSV folder: %s" % self.source_dir)
            self.done["Sources"] = False
            self.done["Bronze"] = False
            self.done["Silver"] = False
            self.done["Gold"] = False
            for stage_name in ("Sources", "Bronze", "Silver", "Gold", "Consume"):
                self.set_stage(stage_name, "idle", "waiting")
            self.log("Selected CSV folder: %s" % self.source_dir)
            self.progress["value"] = 0

    def run_sources(self):
        # A fresh source check invalidates the downstream completion flags.
        self.done["Sources"] = False
        self.done["Bronze"] = False
        self.done["Silver"] = False
        self.done["Gold"] = False
        for stage_name in ("Bronze", "Silver", "Gold", "Consume"):
            self.set_stage(stage_name, "idle", "waiting")
        self.set_stage("Sources", "run", "checking CSVs...")
        self.log("STEP 1 - SOURCES: validating existing CRM and ERP CSV files")
        self.log("Folder: %s" % self.source_dir)
        self.progress["value"] = 10
        try:
            files = validate_source_files(self.source_dir, self.log)
        except (OSError, ValueError, csv.Error) as exc:
            self.done["Sources"] = False
            self.set_stage("Sources", "idle", "CSV files missing / invalid")
            self.log("SOURCE ERROR: %s" % exc)
            self.progress["value"] = 0
            messagebox.showerror("CSV source error", str(exc))
            return False

        time.sleep(0.2)
        self.done["Sources"] = True
        total_rows = sum(info["rows"] for info in files.values())
        self.set_stage("Sources", "done", "%d files, %d rows" % (len(files), total_rows))
        self.progress["value"] = 25
        return True

    def run_bronze(self):
        if not self.need("Sources"):
            return False
        self.set_stage("Bronze", "run", "loading...")
        self.log("STEP 2 - BRONZE: raw load, no transformation, full load (truncate & insert)")
        try:
            load_bronze(self.con, self.log, self.source_dir)
        except (OSError, ValueError, csv.Error, sqlite3.Error) as exc:
            self.done["Bronze"] = False
            self.set_stage("Bronze", "idle", "load failed")
            self.log("BRONZE ERROR: %s" % exc)
            messagebox.showerror("Bronze load error", str(exc))
            return False
        time.sleep(0.4)
        self.done["Bronze"] = True
        t, r = self.count_tables("bronze_")
        self.set_stage("Bronze", "done", "%d tables, %d rows" % (t, r))
        self.progress["value"] = 50
        self.refresh()
        return True

    def run_silver(self):
        if not self.need("Bronze"):
            return False
        self.set_stage("Silver", "run", "cleaning...")
        self.log("STEP 3 - SILVER: cleansing, standardization, normalization, derived columns, enrichment")
        build_silver(self.con, self.log)
        time.sleep(0.4)
        self.done["Silver"] = True
        t, r = self.count_tables("silver_")
        self.set_stage("Silver", "done", "%d tables, %d rows" % (t, r))
        self.progress["value"] = 75
        self.refresh()
        return True

    def run_gold(self):
        if not self.need("Silver"):
            return False
        self.set_stage("Gold", "run", "modeling...")
        self.log("STEP 4 - GOLD: data integration + business logic -> star schema (views, no load)")
        build_gold(self.con, self.log)
        time.sleep(0.4)
        self.done["Gold"] = True
        t, r = self.count_tables("gold_")
        self.set_stage("Gold", "done", "%d views, %d rows" % (t, r))
        self.set_stage("Consume", "done", "BI | SQL | ML ready")
        self.progress["value"] = 100
        self.log("PIPELINE COMPLETE. Open the other tabs to explore the warehouse.")
        self.refresh()
        return True

    def run_all(self):
        if not self.run_sources():
            return
        self.run_bronze()
        if not self.done["Bronze"]:
            return
        self.run_silver()
        if not self.done["Silver"]:
            return
        self.run_gold()

    def reset(self):
        self.con.close()
        os.remove(DB)
        self.con = sqlite3.connect(DB)
        for k in self.done:
            self.done[k] = False
        for name in self.stage_ids:
            self.set_stage(name, "idle", "waiting")
        self.progress["value"] = 0
        self.logbox.delete("1.0", "end")
        self.log("Warehouse reset. Start again from step 1.")
        self.refresh()

    def refresh(self):
        self.fill_table_list()
        self.fill_quality()
        self.fill_characteristics()
        if self.done["Gold"]:
            self.draw_bi()
            self.fill_segments()

    # ------------------------------------------------------------ explorer tab
    def tab_explorer(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="2. Layer Explorer")
        bar = ttk.Frame(tab)
        bar.pack(fill="x", padx=8, pady=8)
        ttk.Label(bar, text="Layer:").pack(side="left")
        self.ex_layer = ttk.Combobox(bar, values=["bronze", "silver", "gold"], state="readonly", width=10)
        self.ex_layer.set("bronze")
        self.ex_layer.pack(side="left", padx=4)
        self.ex_layer.bind("<<ComboboxSelected>>", lambda e: self.fill_table_list())
        ttk.Label(bar, text="Table / View:").pack(side="left", padx=(12, 0))
        self.ex_table = ttk.Combobox(bar, state="readonly", width=34)
        self.ex_table.pack(side="left", padx=4)
        self.ex_table.bind("<<ComboboxSelected>>", lambda e: self.fill_explorer())
        self.ex_info = ttk.Label(bar, text="", font=("Segoe UI", 10, "bold"))
        self.ex_info.pack(side="left", padx=14)
        frame, self.ex_tree = make_tree(tab, 22)
        frame.pack(fill="both", expand=True, padx=8, pady=4)
        self.ex_desc = ttk.Label(tab, text="", wraplength=1200, font=("Segoe UI", 10))
        self.ex_desc.pack(fill="x", padx=8, pady=6)

    def fill_table_list(self):
        layer = self.ex_layer.get()
        names = [r[0] for r in self.con.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE ? AND type IN ('table','view') ORDER BY name",
            (layer + "_%",))]
        self.ex_table["values"] = names
        if names:
            self.ex_table.set(names[0])
            self.fill_explorer()
        else:
            self.ex_table.set("")
            self.ex_tree.delete(*self.ex_tree.get_children())
            self.ex_info.config(text="(layer not built yet)")

    def fill_explorer(self):
        t = self.ex_table.get()
        n = self.con.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
        show(self.ex_tree, self.con.execute("SELECT * FROM %s LIMIT 300" % t))
        self.ex_info.config(text="%d rows (showing first 300)" % n)
        desc = {"bronze": "BRONZE = raw copy of the source file. No cleaning: spaces, duplicates, codes and bad dates are still there. All columns are text.",
                "silver": "SILVER = cleaned & standardized. Duplicates removed, codes decoded, dates converted, history closed, wrong values fixed, audit column dwh_create_date added.",
                "gold": "GOLD = business-ready model (view). Customers and products are integrated from CRM + ERP, with surrogate keys, ready for reporting."}
        self.ex_desc.config(text=desc[self.ex_layer.get()])

    # ------------------------------------------------------------ raw vs clean tab
    def tab_compare(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="3. Raw vs Clean")
        bar = ttk.Frame(tab)
        bar.pack(fill="x", padx=8, pady=8)
        ttk.Label(bar, text="Entity:").pack(side="left")
        self.entities = {
            "Customers (CRM)": ("bronze_crm_cust_info", "silver_crm_cust_info"),
            "Products (CRM)": ("bronze_crm_prd_info", "silver_crm_prd_info"),
            "Sales (CRM)": ("bronze_crm_sales_details", "silver_crm_sales_details"),
            "Customer demographics (ERP)": ("bronze_erp_cust_az12", "silver_erp_cust_az12"),
            "Customer location (ERP)": ("bronze_erp_loc_a101", "silver_erp_loc_a101"),
            "Product categories (ERP)": ("bronze_erp_px_cat_g1v2", "silver_erp_px_cat_g1v2"),
        }
        self.cmp_ent = ttk.Combobox(bar, values=list(self.entities), state="readonly", width=34)
        self.cmp_ent.set("Customers (CRM)")
        self.cmp_ent.pack(side="left", padx=6)
        ttk.Button(bar, text="Compare", command=self.fill_compare).pack(side="left")
        ttk.Label(bar, text="  (same source rows, before and after preprocessing)").pack(side="left")
        tk.Label(tab, text="BRONZE (raw)", bg=C_BRONZE, font=("Segoe UI", 11, "bold")).pack(fill="x", padx=8)
        f1, self.cmp_b = make_tree(tab, 10)
        f1.pack(fill="both", expand=True, padx=8, pady=2)
        tk.Label(tab, text="SILVER (cleaned)", bg=C_SILVER, font=("Segoe UI", 11, "bold")).pack(fill="x", padx=8)
        f2, self.cmp_s = make_tree(tab, 10)
        f2.pack(fill="both", expand=True, padx=8, pady=2)
        self.cmp_note = ttk.Label(tab, text="", wraplength=1200, font=("Segoe UI", 10))
        self.cmp_note.pack(fill="x", padx=8, pady=6)

    def fill_compare(self):
        if not self.done["Silver"]:
            messagebox.showinfo("Info", "Build the Silver layer first.")
            return
        b, s = self.entities[self.cmp_ent.get()]
        show(self.cmp_b, self.con.execute("SELECT * FROM %s LIMIT 60" % b))
        show(self.cmp_s, self.con.execute("SELECT * FROM %s LIMIT 60" % s))
        nb = self.con.execute("SELECT COUNT(*) FROM " + b).fetchone()[0]
        ns = self.con.execute("SELECT COUNT(*) FROM " + s).fetchone()[0]
        notes = {"Customers (CRM)": "Fixed: duplicates removed (latest record kept), missing IDs dropped, names trimmed, M/F/S decoded to Male/Female/Married/Single.",
                 "Products (CRM)": "Fixed: category id and product key split from one key, cost null -> 0, product line decoded, end date derived from next version start date (history).",
                 "Sales (CRM)": "Fixed: integer dates (YYYYMMDD) converted to dates, invalid dates -> NULL, sales recalculated as quantity x price, missing/negative price derived.",
                 "Customer demographics (ERP)": "Fixed: 'NAS' prefix removed to match CRM key, future birth dates -> NULL, gender normalized.",
                 "Customer location (ERP)": "Fixed: dash removed from id to match CRM key, country codes (DE, US, USA) expanded, blanks -> n/a.",
                 "Product categories (ERP)": "Mostly clean: only whitespace trimming and audit column added."}
        self.cmp_note.config(text="Rows: bronze %d  ->  silver %d.   %s" % (nb, ns, notes[self.cmp_ent.get()]))

    # ------------------------------------------------------------ quality tab
    def tab_quality(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="4. Data Quality")
        ttk.Label(tab, text="Data quality checks: number of problem records BEFORE (Bronze) and AFTER (Silver) preprocessing",
                  font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=8, pady=8)
        frame, self.q_tree = make_tree(tab, 16)
        frame.pack(fill="both", expand=True, padx=8, pady=4)
        self.q_tree["columns"] = ("check", "bronze", "silver", "status")
        for c, w in (("check", 520), ("bronze", 140), ("silver", 140), ("status", 160)):
            self.q_tree.heading(c, text=c.upper())
            self.q_tree.column(c, width=w, anchor="w" if c == "check" else "center")
        self.q_sum = ttk.Label(tab, text="", font=("Segoe UI", 11, "bold"))
        self.q_sum.pack(anchor="w", padx=8, pady=8)

    def fill_quality(self):
        self.q_tree.delete(*self.q_tree.get_children())
        if not self.done["Silver"]:
            self.q_sum.config(text="Run Bronze and Silver to see the checks.")
            return
        tb = ts = 0
        for name, sb, ss in QUALITY:
            b = self.con.execute(sb).fetchone()[0]
            s = self.con.execute(ss).fetchone()[0]
            tb += b
            ts += s
            self.q_tree.insert("", "end", values=(name, b, s, "FIXED" if s == 0 and b > 0 else ("OK" if b == 0 else "CHECK")))
        if self.done["Gold"]:
            o = self.con.execute("SELECT COUNT(*) FROM gold_fact_sales WHERE customer_key IS NULL OR product_key IS NULL").fetchone()[0]
            self.q_tree.insert("", "end", values=("Gold: fact rows without customer/product (referential integrity)", "-", o,
                                                  "OK" if o == 0 else "ORPHANS"))
        self.q_sum.config(text="Problem records: Bronze = %d   ->   Silver = %d" % (tb, ts))

    # ------------------------------------------------------------ model tab
    def tab_model(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="5. Gold Data Model")
        cv = tk.Canvas(tab, width=1230, height=620, bg="white")
        cv.pack(padx=8, pady=8)
        boxes = {
            "gold_dim_customers (DIMENSION)": (40, 60, ["customer_key  (PK)", "customer_id", "customer_number", "first_name", "last_name",
                                                         "country  (from ERP)", "marital_status", "gender  (CRM + ERP)", "birthdate  (ERP)", "create_date"]),
            "gold_dim_products (DIMENSION)": (880, 60, ["product_key  (PK)", "product_id", "product_number", "product_name", "category  (ERP)",
                                                         "subcategory  (ERP)", "maintenance", "cost", "product_line", "start_date"]),
            "gold_fact_sales (FACT)": (460, 200, ["order_number", "product_key  (FK)", "customer_key  (FK)", "order_date", "shipping_date",
                                                   "due_date", "sales_amount", "quantity", "price"]),
        }
        for title, (x, y, cols) in boxes.items():
            fact = "FACT" in title
            cv.create_rectangle(x, y, x + 310, y + 40 + 24 * len(cols), fill="#FFF2CC" if fact else "#DDEBF7", outline="#333", width=2)
            cv.create_rectangle(x, y, x + 310, y + 34, fill="#F4B183" if fact else "#9DC3E6", outline="#333", width=2)
            cv.create_text(x + 155, y + 17, text=title, font=("Segoe UI", 10, "bold"))
            for i, c in enumerate(cols):
                cv.create_text(x + 14, y + 52 + i * 24, text=c, anchor="w", font=("Consolas", 10))
        cv.create_line(350, 270, 460, 270, arrow="both", width=3, fill="#2E75B6")
        cv.create_line(770, 270, 880, 270, arrow="both", width=3, fill="#2E75B6")
        cv.create_text(405, 255, text="1 : N", font=("Segoe UI", 9, "bold"))
        cv.create_text(825, 255, text="1 : N", font=("Segoe UI", 9, "bold"))
        cv.create_text(615, 470, text="STAR SCHEMA: one fact table (measures) surrounded by dimension tables (descriptions)",
                       font=("Segoe UI", 11, "bold"))
        cv.create_text(615, 500, text="Also in Gold:  gold_flat_sales (flat table)   |   gold_agg_monthly_sales (aggregated table)",
                       font=("Segoe UI", 10))
        cv.create_text(615, 530, text="Gold objects are VIEWS on top of Silver  ->  'No Load' step, always up to date.",
                       font=("Segoe UI", 10))

    # ------------------------------------------------------------ consume tab
    def tab_consume(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="6. Consume (BI / SQL / ML)")
        sub = ttk.Notebook(tab)
        sub.pack(fill="both", expand=True, padx=4, pady=4)

        bi = ttk.Frame(sub)
        sub.add(bi, text="BI & Reporting")
        self.kpi = ttk.Label(bi, text="Run the pipeline to see KPIs.", font=("Segoe UI", 12, "bold"))
        self.kpi.pack(anchor="w", padx=10, pady=8)
        row = ttk.Frame(bi)
        row.pack(fill="x")
        self.cv_country = tk.Canvas(row, width=600, height=300, bg="white", highlightthickness=1, highlightbackground="#bbb")
        self.cv_country.pack(side="left", padx=8, pady=4)
        self.cv_cat = tk.Canvas(row, width=600, height=300, bg="white", highlightthickness=1, highlightbackground="#bbb")
        self.cv_cat.pack(side="left", padx=8, pady=4)
        self.cv_month = tk.Canvas(bi, width=1210, height=250, bg="white", highlightthickness=1, highlightbackground="#bbb")
        self.cv_month.pack(padx=8, pady=4)

        sq = ttk.Frame(sub)
        sub.add(sq, text="Ad-hoc SQL")
        self.examples = {
            "Top 10 customers by sales": "SELECT customer, country, ROUND(SUM(sales_amount),2) AS total FROM gold_flat_sales GROUP BY customer, country ORDER BY total DESC LIMIT 10",
            "Sales by category": "SELECT category, subcategory, ROUND(SUM(sales_amount),2) AS total FROM gold_flat_sales GROUP BY category, subcategory ORDER BY total DESC",
            "Monthly sales (aggregated table)": "SELECT * FROM gold_agg_monthly_sales ORDER BY month",
            "Sales by gender and country": "SELECT country, gender, COUNT(*) AS orders FROM gold_flat_sales GROUP BY country, gender ORDER BY orders DESC",
            "Product history (silver)": "SELECT prd_key, prd_nm, prd_start_dt, prd_end_dt FROM silver_crm_prd_info WHERE prd_key IN (SELECT prd_key FROM silver_crm_prd_info GROUP BY prd_key HAVING COUNT(*)>1) ORDER BY prd_key, prd_start_dt",
        }
        top = ttk.Frame(sq)
        top.pack(fill="x", padx=8, pady=6)
        ttk.Label(top, text="Example:").pack(side="left")
        self.sql_ex = ttk.Combobox(top, values=list(self.examples), state="readonly", width=40)
        self.sql_ex.pack(side="left", padx=6)
        self.sql_ex.bind("<<ComboboxSelected>>", self.pick_example)
        ttk.Button(top, text="Run SQL", style="Big.TButton", command=self.run_sql).pack(side="left", padx=8)
        self.sql_text = tk.Text(sq, height=5, font=("Consolas", 11))
        self.sql_text.pack(fill="x", padx=8)
        self.sql_text.insert("1.0", "SELECT * FROM gold_fact_sales LIMIT 20")
        self.sql_msg = ttk.Label(sq, text="")
        self.sql_msg.pack(anchor="w", padx=8)
        f, self.sql_tree = make_tree(sq, 16)
        f.pack(fill="both", expand=True, padx=8, pady=4)

        ml = ttk.Frame(sub)
        sub.add(ml, text="Machine Learning")
        ttk.Label(ml, text="Customer segmentation with k-means (k = 3) on Gold data: total spend vs. number of orders",
                  font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=10, pady=8)
        self.cv_ml = tk.Canvas(ml, width=620, height=380, bg="white", highlightthickness=1, highlightbackground="#bbb")
        self.cv_ml.pack(side="left", padx=10, pady=4)
        right = ttk.Frame(ml)
        right.pack(side="left", fill="both", expand=True, padx=8)
        f2, self.ml_tree = make_tree(right, 6)
        f2.pack(fill="x", pady=8)
        ttk.Label(right, text="The Gold layer feeds ML directly: one clean row per customer,\nno extra cleaning needed in the model code.",
                  font=("Segoe UI", 10)).pack(anchor="w")

    def pick_example(self, event):
        self.sql_text.delete("1.0", "end")
        self.sql_text.insert("1.0", self.examples[self.sql_ex.get()])

    def run_sql(self):
        try:
            cur = self.con.execute(self.sql_text.get("1.0", "end").strip().rstrip(";"))
            if cur.description is None:
                self.sql_msg.config(text="Statement executed.")
                return
            show(self.sql_tree, cur)
            self.sql_msg.config(text="%d rows returned" % len(self.sql_tree.get_children()))
        except sqlite3.Error as e:
            self.sql_msg.config(text="SQL error: %s" % e)

    def draw_bi(self):
        c = self.con
        tot, orders, custs = c.execute("SELECT SUM(sales_amount), COUNT(DISTINCT order_number), COUNT(DISTINCT customer_key) FROM gold_fact_sales").fetchone()
        self.kpi.config(text="Total sales: {:,.0f}      Orders: {:,}      Active customers: {:,}      Avg order: {:,.0f}".format(tot, orders, custs, tot / orders))
        r = c.execute("SELECT country, SUM(sales_amount) FROM gold_flat_sales GROUP BY country ORDER BY 2 DESC LIMIT 8").fetchall()
        draw_bars(self.cv_country, "Sales by country", [x[0] for x in r], [x[1] for x in r], "#5B9BD5")
        r = c.execute("SELECT category, SUM(sales_amount) FROM gold_flat_sales GROUP BY category ORDER BY 2 DESC").fetchall()
        draw_bars(self.cv_cat, "Sales by product category", [x[0] for x in r], [x[1] for x in r], "#ED7D31")
        r = c.execute("SELECT month, total_sales FROM gold_agg_monthly_sales ORDER BY month").fetchall()
        cv = self.cv_month
        cv.delete("all")
        cv.create_text(10, 12, text="Monthly sales trend (gold_agg_monthly_sales)", anchor="w", font=("Segoe UI", 11, "bold"))
        top = max(x[1] for x in r)
        pts = []
        for i, (m, v) in enumerate(r):
            x = 50 + i * (1130 / max(1, len(r) - 1))
            y = 220 - 170 * v / top
            pts.append((x, y))
            cv.create_oval(x - 3, y - 3, x + 3, y + 3, fill="#2E75B6", outline="")
            if i % 3 == 0:
                cv.create_text(x, 235, text=m, font=("Segoe UI", 8))
        for a, b in zip(pts, pts[1:]):
            cv.create_line(a[0], a[1], b[0], b[1], fill="#2E75B6", width=2)

    def fill_segments(self):
        rows = self.con.execute("SELECT customer_key, SUM(sales_amount), COUNT(DISTINCT order_number) FROM gold_fact_sales "
                                "WHERE customer_key IS NOT NULL GROUP BY customer_key").fetchall()
        mx, mo = max(r[1] for r in rows), max(r[2] for r in rows)
        pts = [(r[1] / mx, r[2] / mo) for r in rows]
        random.seed(1)
        cent = random.sample(pts, 3)
        for _ in range(30):
            groups = [[] for _ in range(3)]
            for i, p in enumerate(pts):
                j = min(range(3), key=lambda k: (p[0] - cent[k][0]) ** 2 + (p[1] - cent[k][1]) ** 2)
                groups[j].append(i)
            cent = [(sum(pts[i][0] for i in g) / len(g), sum(pts[i][1] for i in g) / len(g)) if g else cent[k]
                    for k, g in enumerate(groups)]
        order = sorted(range(3), key=lambda k: cent[k][0] + cent[k][1])
        names = {order[0]: "Low value", order[1]: "Medium value", order[2]: "High value"}
        colors = {order[0]: "#A5A5A5", order[1]: "#5B9BD5", order[2]: "#ED7D31"}
        cv = self.cv_ml
        cv.delete("all")
        cv.create_line(50, 340, 600, 340, width=2)
        cv.create_line(50, 340, 50, 20, width=2)
        cv.create_text(325, 365, text="total spend", font=("Segoe UI", 9))
        cv.create_text(20, 180, text="orders", angle=90, font=("Segoe UI", 9))
        for k, g in enumerate(groups):
            for i in g:
                x, y = 50 + pts[i][0] * 540, 340 - pts[i][1] * 310
                cv.create_oval(x - 3, y - 3, x + 3, y + 3, fill=colors[k], outline="")
        for k in order:
            cv.create_text(70, 30 + order.index(k) * 18, text="● " + names[k], anchor="w", fill=colors[k], font=("Segoe UI", 10, "bold"))
        self.ml_tree.delete(*self.ml_tree.get_children())
        self.ml_tree["columns"] = ("segment", "customers", "avg_spend", "avg_orders")
        for c in self.ml_tree["columns"]:
            self.ml_tree.heading(c, text=c)
            self.ml_tree.column(c, width=120)
        for k in reversed(order):
            g = groups[k]
            if g:
                self.ml_tree.insert("", "end", values=(names[k], len(g), "{:,.0f}".format(sum(rows[i][1] for i in g) / len(g)),
                                                       "%.1f" % (sum(rows[i][2] for i in g) / len(g))))

    # ------------------------------------------------------------ characteristics tab
    def tab_characteristics(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="7. DW Characteristics")
        ttk.Label(tab, text="Characteristics of a Data Warehouse - with live evidence from this project",
                  font=("Segoe UI", 13, "bold")).pack(anchor="w", padx=10, pady=8)
        self.char_boxes = {}
        grid = ttk.Frame(tab)
        grid.pack(fill="both", expand=True, padx=8)
        titles = [("Subject-oriented", "Organized around business subjects (here: SALES, CUSTOMERS, PRODUCTS), not around the applications that produce the data."),
                  ("Integrated", "Data from different sources (CRM + ERP) is combined with one consistent format, naming, codes and keys."),
                  ("Time-variant", "Keeps history, so we can analyze changes over time (dates, product versions, trends)."),
                  ("Non-volatile", "Data is loaded and read, not changed by users. Each batch replaces the layer completely (truncate & insert) - the source is never modified."),
                  ("Separation from operational systems", "Analytics run on the warehouse (SQLite file), so CRM/ERP systems are not slowed down."),
                  ("Layered architecture", "Bronze (raw) -> Silver (clean) -> Gold (business) gives traceability: every Gold number can be traced back to the raw file.")]
        for i, (t, d) in enumerate(titles):
            box = tk.LabelFrame(grid, text=t, font=("Segoe UI", 11, "bold"), padx=8, pady=6, fg="#1F3864")
            box.grid(row=i // 2, column=i % 2, sticky="nsew", padx=6, pady=6)
            grid.columnconfigure(i % 2, weight=1)
            grid.rowconfigure(i // 2, weight=1)
            tk.Label(box, text=d, wraplength=560, justify="left", font=("Segoe UI", 10)).pack(anchor="w")
            ev = tk.Label(box, text="Evidence: (run the pipeline)", wraplength=560, justify="left", fg="#2E7D32",
                          font=("Segoe UI", 10, "bold"))
            ev.pack(anchor="w", pady=(6, 0))
            self.char_boxes[t] = ev

    def fill_characteristics(self):
        c, cb = self.con, self.char_boxes
        if self.done["Bronze"]:
            nb = c.execute("SELECT COUNT(*) FROM bronze_crm_sales_details").fetchone()[0]
            cb["Non-volatile"].config(text="Evidence: bronze sales = %d rows. Press 'Load Bronze' again: the count stays %d (no duplicates, no overwriting of history in the source)." % (nb, nb))
            cb["Separation from operational systems"].config(text="Evidence: source = external CSV files in %s; warehouse = %s (queries never modify the source files)." % (self.source_dir, os.path.basename(DB)))
        if self.done["Silver"]:
            cb["Integrated"].config(text="Evidence: CRM customer id 'AW00011000' and ERP ids 'NASAW00011000' / 'AW-00011000' were unified into one key; "
                                         "'DE','US','USA' became 'Germany','United States'; M/F became Male/Female.")
            v = c.execute("SELECT COUNT(*) FROM silver_crm_prd_info WHERE prd_end_dt IS NOT NULL").fetchone()[0]
            cb["Time-variant"].config(text="Evidence: %d product versions are closed with an end date (history kept). Sales dates are stored for trend analysis." % v)
        if self.done["Gold"]:
            a, b = c.execute("SELECT MIN(order_date), MAX(order_date) FROM gold_fact_sales").fetchone()
            cb["Time-variant"].config(text=cb["Time-variant"].cget("text") + " Sales span %s to %s." % (a, b))
            cb["Subject-oriented"].config(text="Evidence: Gold exposes 2 dimensions + 1 fact (star schema) + flat + aggregated view, all about Sales.")
            n = len(c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')").fetchall())
            cb["Layered architecture"].config(text="Evidence: %d objects: 6 bronze tables, 6 silver tables, 5 gold views." % n)


if __name__ == "__main__":
    root = tk.Tk()
    AuthGate(root)
    root.mainloop()
