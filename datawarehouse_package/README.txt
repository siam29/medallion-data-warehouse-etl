DATA WAREHOUSE DEMO - COMPLETE PACKAGE
======================================

Package contents
----------------
- datawarehouse.py: Python desktop application
- datasets/source_crm/: 3 CRM source CSV files
- datasets/source_erp/: 3 ERP source CSV files

The application reads the six CSV files. It does not generate, overwrite,
or delete the source CSV files.

CSV files included
------------------
CRM:
  cust_info.csv
  prd_info.csv
  sales_details.csv

ERP:
  CUST_AZ12.csv
  LOC_A101.csv
  PX_CAT_G1V2.csv

How to run
----------
1. Extract the ZIP file.
2. Keep datawarehouse.py and the datasets folder together.
3. Run from a terminal in the extracted folder:

     python ml.py

4. In the application, the included datasets folder is selected by default.
5. Click RUN FULL PIPELINE, or run the four pipeline stages one by one.

If you want to use another folder of CSV files, click Select CSV Folder
and choose the folder containing source_crm and source_erp (or the folder
containing all six CSV files directly).

Requirements
------------
- Python 3.9 or later recommended
- Tkinter (GUI)
- SQLite (included with standard Python)

No pip packages are required. On Ubuntu/Debian, if Tkinter is missing:

     sudo apt install python3-tk

Notes
-----
- The included CSVs are sample CRM/ERP data with intentional data-quality
  issues, so the Bronze-to-Silver cleaning steps can be demonstrated.
- The warehouse database is created as datawarehouse.db beside the script.
- Source CSV files are read-only from the application's perspective.
