# config_paths.py
from pathlib import Path
import os
#from config import PATHS, JDBC

# === PATHS ===
PATHS = {
    "purchdata": Path(r"path\to\sqlite\database.sqlite"),
    "saamms": Path(r"path\to\general\mass_maint\folder"),
    "count_flag_data": Path(r"path\to\count_flag_data"),
    "icsl_data": Path(r"path\to\icsl\data\folder"),
    "sups_nlas_data": Path(r"path\to\sup\nla\data\folder"),
    "oantostock": Path(r"path\to\folder\which\contains\xlsx"), #most recent .xlsx in folder should be column A = whse, column B = prod, where all prod / warehouse combos listed should be converted to status STOCK in ICSW
    "threshold_data":Path(r"path\to\folder\which\containing\threshold\data"),
    "whse_rank_data": Path(r"path\to\whse\rank\data"),
    "dnr": Path(r"path\to\dnr\data\folder"),
    "icsw_maint_spreadsheet": (r"path\to\monthly icsw maintenance guide.xlsx"), #contains usage control rules, arppath exceptions
    "warehouse_info_csv": (r"path\to\whseinfo.csv"), #contains warehouse information relating to correct product / product line setup
}

# === JDBC CONFIG ===
JDBC = {
    "class": "",
    "url": "",
    "jar": r"",
    "user": "",
    "password": "",
}

# === EMAILS ===

EMAILS = {
    "mass_maint_user":"myemail@company.com",
    "sup_nla_notification_emails": ["person1@company.com", "person2@company.com"]
}

GMAIL_CREDS = {
    "DEFAULT_TO":["myemail@company.com"], #  default recipient(s) for gmail notifications
    "GMAIL_USER":"mygmail@gmail.com",     #  your Gmail address ; must create app password and set it as environmental variable
    "GMAIL_APP_PASSWORD":os.getenv("GMAIL_APP_PASSWORD"),  # load from env variable
}