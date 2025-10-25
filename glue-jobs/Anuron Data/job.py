import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import smtplib
from email.message import EmailMessage
from openpyxl import Workbook
from tempfile import NamedTemporaryFile

# ---------------- CONFIG ----------------
bucket_name = "streamlit-app-server-bucket"
file_key = "Leads/BASEDATA.xlsx"

file_name_key = "Leads/CompleteLeadsData.xlsx"

required_hubs = [
    "REVOLT HUB AHMEDNAGAR", "REVOLT HUB ANDHERI", "REVOLT HUB KOLHAPUR",
    "REVOLT HUB NASHIK", "REVOLT HUB PCMC", "REVOLT HUB SINGHAD ROAD"
]
from_email = "bi@rattanindia.com"
email_password = "Power@2024"


to_emails = ["operationshead@anuron.co.in"]
cc_emails = ["rajat.kapoor@rattanindia.com","kamal@soham.co.in","shivam.choudhary@rattanindia.com","himanshu.ratta@revoltmotors.com","saurabh.sinha@revoltmotors.com","umesh.arora@revoltmotors.com"]

# ---------------- DATE CONFIG ----------------
start_month = "2023-06"
end_month = datetime.today().strftime("%Y-%m")
month_range = pd.period_range(start=start_month, end=end_month, freq="M").astype(str)
month_labels = {m: pd.to_datetime(m).strftime("%b'%y") for m in month_range}
all_months = list(month_labels.values())

# ---------------- READ FROM S3 ----------------
s3 = boto3.client("s3")
obj = s3.get_object(Bucket=bucket_name, Key=file_key)
df = pd.read_excel(BytesIO(obj["Body"].read()), engine="openpyxl")

s3 = boto3.client("s3")
obj = s3.get_object(Bucket=bucket_name, Key=file_name_key)
df_name = pd.read_excel(BytesIO(obj["Body"].read()), engine="openpyxl")
df_name = df_name[['OpportunityId','mx_Custom_1']]

df.rename(columns={"opportunityid": "OpportunityId"}, inplace=True)
df = df.merge(df_name, on='OpportunityId', how='left')

# ---------------- CLEANUP ----------------
df = df[df["hub"].isin(required_hubs)]
for col in ["createdondate", "bookingdatelsq", "retaildatelsq"]:
    df[col] = pd.to_datetime(df[col], errors="coerce")
df["CreatedOn_D_Month"] = df["createdondate"].dt.to_period("M").astype(str)
df["MonthLabel"] = df["CreatedOn_D_Month"].map(lambda x: pd.to_datetime(x).strftime("%b'%y"))

# ---------------- TABLE 1: Summary ----------------
sum_df = df[["createdondate", "bookingdatelsq", "retaildatelsq"]].copy()
sum_df["Month"] = sum_df["createdondate"].dt.to_period("M").astype(str).map(month_labels)
sum_df["Leads"] = 1
sum_df["Bookings"] = sum_df["bookingdatelsq"].notna().astype(int)
sum_df["Retails"] = sum_df["retaildatelsq"].notna().astype(int)

t1 = sum_df.groupby("Month")[["Leads", "Bookings", "Retails"]].sum().reindex(all_months, fill_value=0)
t1["Booking Conversion %"] = (t1["Bookings"] / t1["Leads"].replace(0, 1) * 100).round().astype(int).astype(str) + '%'
t1["Retail Conversion %"] = (t1["Retails"] / t1["Leads"].replace(0, 1) * 100).round().astype(int).astype(str) + '%'
table1 = t1[["Leads", "Bookings", "Booking Conversion %", "Retails", "Retail Conversion %"]].T

# ---------------- TABLE 2: Retail by Source ----------------
df["RetailMonth"] = df["createdondate"].dt.to_period("M").astype(str).map(month_labels)
retail_source = df[df["retaildatelsq"].notna()]
table2 = retail_source.groupby(["revsourcetype", "RetailMonth"]).size().unstack(fill_value=0).reindex(columns=all_months, fill_value=0)

# ---------------- TABLE 3: Lead Not Attended by Source ----------------
lead_not_attended = df[df["attendedstatus"] == "Lead Not Attended"]
table3 = lead_not_attended.groupby(["revsourcetype", "RetailMonth"]).size().unstack(fill_value=0).reindex(columns=all_months, fill_value=0)

# ---------------- RAW EXCEL EXPORT ----------------
columns_to_keep = {
    "createdondate": "Lead Created Date",
     "mx_Custom_1":"Customer Name",
    "mobilenumber": "Mobile Number",
    "leadowner": "Lead Owner",
    "rawmainstage": "Lead Main Stage",
    "bookingdatelsq": "Booking Date",
    "retaildatelsq": "Retail Date",
    "trcompleteddate": "Test Ride Completed",
    "attendedstatus": "Lead Attended Status",
    "revsourcetype": "Lead Source",
    "hub": "Hub Name",
    "opportunityid": "OpportunityId"
}

# ✅ Step 1: Filter only available columns from df
available_columns = [col for col in columns_to_keep if col in df.columns]
missing_columns = [col for col in columns_to_keep if col not in df.columns]

if missing_columns:
    print(f"⚠️ Warning: Missing columns in DataFrame and will be skipped: {missing_columns}")

# ✅ Step 2: Subset and rename
raw_export_df = df[available_columns].rename(columns={col: columns_to_keep[col] for col in available_columns})


# ✅ Step 4:Export to Excel
with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
    raw_export_df.to_excel(tmp.name, index=False)
    attachment_path = tmp.name

print(f"✅ Raw Excel export prepared at: {attachment_path}")

# ---------------- HTML FORMATTING ----------------
def render_html_table(df, title):
    return f"""
    <h3>{title}</h3>
    {df.to_html(classes='styled-table', border=1, justify='center')}
    <br><br>
    """

html_content = f"""
<html>
  <head>
    <style>
      .styled-table {{
        font-family: Arial, sans-serif;
        border-collapse: collapse;
        width: 100%;
        font-size: 14px;
      }}
      .styled-table th {{
        background-color: #007BFF;
        color: white;
        text-align: center;
        padding: 8px;
      }}
      .styled-table td {{
        border: 1px solid #ddd;
        text-align: center;
        padding: 8px;
      }}
      .styled-table tr:nth-child(even) {{ background-color: #f9f9f9; }}
    </style>
  </head>
  <body>
    <p>Hi Team,</p>
    <p>Please find below the Monthly Lead Summary Report (Jun'23 to {month_labels[end_month]}):</p>
    {render_html_table(table1, 'Table 1: Leads, Bookings & Retail Summary')}
    {render_html_table(table2, 'Table 2: Retail by REV Source Type')}
    {render_html_table(table3, 'Table 3: Lead Not Attended by REV Source Type')}
    <p>Regards,<br>BI Team</p>
  </body>
</html>
"""

# ---------------- EMAIL WITH ATTACHMENT ----------------
msg = EmailMessage()
msg["Subject"] = "Monthly Lead Summary Report - Anuron Enterprise"
msg["From"] = from_email
msg["To"] = ", ".join(to_emails)
msg["Cc"] = ", ".join(cc_emails)
msg.set_content("This email contains an HTML-formatted summary table and raw Excel export.")
msg.add_alternative(html_content, subtype="html")

with open(attachment_path, "rb") as f:
    file_data = f.read()
    file_name = "Raw_Data_Anuron_Enterprise.xlsx"
    msg.add_attachment(file_data, maintype="application", subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=file_name)

with smtplib.SMTP("smtp.office365.com", 587) as server:
    server.starttls()
    server.login(from_email, email_password)
    server.send_message(msg)

print("✅ Email with attachment sent successfully!")
