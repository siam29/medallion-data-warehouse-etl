# medallion-data-warehouse-etl

A desktop data warehouse application built in Python. It loads six CRM and ERP CSV files through a Bronze → Silver → Gold (medallion) pipeline on SQLite, then uses the clean data for BI charts, ad-hoc SQL, customer segmentation (k-means) and a 3 to 5 year sales forecast. A secure login with admin approval protects access.


## Project Overview

| Item | Details |
|---|---|
| **Goal** | Show a complete data warehouse: raw CSV to business insight, BI and ML |
| **Language** | Python 3.8+ |
| **UI** | Tkinter (tk, ttk, messagebox, filedialog) |
| **Database** | SQLite (window functions need SQLite 3.25+) |
| **Architecture** | Medallion: Bronze (raw), Silver (clean), Gold (business views) |
| **Sources** | CRM (3 CSV files) and ERP (3 CSV files) |
| **ML** | K-means (descriptive) and regression forecast (predictive), pure Python |
| **Charts** | Drawn on a Tkinter Canvas, no matplotlib |
| **Dependencies** | None beyond the Python standard library |

## Run this code 
``` python ml.py ```
