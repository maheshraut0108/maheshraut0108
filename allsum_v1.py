import os
import base64
import boto3
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly','https://www.googleapis.com/auth/spreadsheets']

#SHEET_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

SPREADSHEET_ID = "1jFfq5EENULPrrvYa9grH3gbmB61_yhpaOQZ1FrEtU-A"
SHEET_NAME = "cloud.druva.com"
SHEET_GOV = "Main Sheet"
SHEET_FEDRAMP = "FedRAMP Charts"

def authenticate_gmail():
    """Authenticate and return the Gmail API service"""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def search_emails(service, query):
    """Search for emails matching the query"""
    try:
        results = service.users().messages().list(userId='me', q=query).execute()
        messages = results.get('messages', [])
        return messages
    except HttpError as error:
        print(f'An error occurred: {error}')
        return []

def download_attachments(service, message_id, folder_path, subject):
    """Download attachments from the email and rename based on subject"""
    try:
        message = service.users().messages().get(userId='me', id=message_id).execute()
        date = message['internalDate']
        date_str = datetime.fromtimestamp(int(date) / 1000).strftime('%Y-%m-%d')
        for part in message['payload'].get('parts', []):
            if part['filename']:
                # Log the subject and filename being set
                print(f'Subject: {subject}')
                
                if "AP1" not in subject and "inSyncGovCloud" not in subject:
                    filename = f'us0_Complete_Report_{date_str}.csv'
                elif "AP1" in subject:
                    filename = f'ap1_Complete_Report_{date_str}.csv'
                elif "inSyncGovCloud" in subject:
                    filename = f'gov_Complete_Report_{date_str}.csv'
                else:
                    filename = f'{date_str}_{part["filename"]}'
                
                # Log the filename being set
                print(f'Downloading attachment as: {filename}')
                
                if 'data' in part['body']:
                    data = part['body']['data']
                else:
                    att_id = part['body']['attachmentId']
                    att = service.users().messages().attachments().get(userId='me', messageId=message_id, id=att_id).execute()
                    data = att['data']
                file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                
                path = os.path.join(folder_path, filename)
                with open(path, 'wb') as f:
                    f.write(file_data)
    except HttpError as error:
        print(f'An error occurred: {error}')

def extract_date_from_subject(subject):
    """Extract date from the email subject"""
    try:
        date_str = subject.split(' on ')[-1]
        return datetime.strptime(date_str, '%d %b %Y')
    except ValueError:
        return None

def get_previous_month_storage_usage():
    # Initialize a session using Amazon Cost Explorer with a specific profile
    session = boto3.Session(profile_name='payer')
    client = session.client('ce')

    # Calculate the previous month date range
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

    start_date = first_day_of_previous_month.strftime('%Y-%m-%d')
    end_date = first_day_of_current_month.strftime('%Y-%m-%d')

    # Create a cost explorer query with filters
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=['UsageQuantity'],
        GroupBy=[
            {
                'Type': 'TAG',
                'Key': 'druva:service'
            }
        ],
        Filter={
            'And': [
                {
                    'Dimensions': {
                        'Key': 'LINKED_ACCOUNT',
                        'Values': ['020502556137', '257519804744', '221522429396']
                    }
                },
                {
                    'Not': {
                        'Dimensions': {
                            'Key': 'REGION',
                            'Values': ['us-gov-west-1']
                        }
                    }
                },
                {
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': ['Amazon Simple Storage Service']
                    }
                },
                {
                    'Tags': {
                        'Key': 'druva:service',
                        'Values': ['storage']
                    }
                },
                {
                    'Dimensions': {
                        'Key': 'USAGE_TYPE_GROUP',
                        'Values': [
                            'S3: Storage - Standard',
                            'S3: Storage - Standard Infrequent Access'
                        ]
                    }
                },
                {
                    'Not': {
                        'Dimensions': {
                            'Key': 'RECORD_TYPE',
                            'Values': ['Credit', 'Refund']
                        }
                    }
                }
            ]
        }
    )

    # Parse the response to get storage usage values
    results = response.get('ResultsByTime', [])
    if not results:
        print("No data found for the specified time period.")
        return

    for result in results:
        groups = result.get('Groups', [])
        if not groups:
            continue
        for group in groups:
            metrics = group.get('Metrics', {})
            usage_quantity = metrics.get('UsageQuantity', {}).get('Amount', '0')
            if float(usage_quantity) > 0:
                print(f"S3 As per CE: {usage_quantity} GB")
    return usage_quantity

def get_previous_month_s3_gir_usage():
    # Initialize a session using Amazon Cost Explorer with a specific profile
    session = boto3.Session(profile_name='payer')
    client = session.client('ce')

    # Calculate the previous month date range
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    first_day_of_previous_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)

    start_date = first_day_of_previous_month.strftime('%Y-%m-%d')
    end_date = first_day_of_current_month.strftime('%Y-%m-%d')

    # Create a cost explorer query with filters
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=['UsageQuantity'],
        GroupBy=[
            {
                'Type': 'TAG',
                'Key': 'druva:service'
            }
        ],
        Filter={
            'And': [
                {
                    'Dimensions': {
                        'Key': 'LINKED_ACCOUNT',
                        'Values': ['020502556137', '257519804744', '221522429396']
                    }
                },
                {
                    'Not': {
                        'Dimensions': {
                            'Key': 'REGION',
                            'Values': ['us-gov-west-1']
                        }
                    }
                },
                {
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': ['Amazon Simple Storage Service']
                    }
                },
                {
                    'Dimensions': {
                        'Key': 'USAGE_TYPE',
                        'Values': [
                            'AFS1-TimedStorage-GIR-ByteHrs', 'APE1-TimedStorage-GIR-ByteHrs', 
                            'APN1-TimedStorage-GIR-ByteHrs', 'APN2-TimedStorage-GIR-ByteHrs',
                            'APS1-TimedStorage-GIR-ByteHrs', 'APS2-TimedStorage-GIR-ByteHrs', 
                            'APS3-TimedStorage-GIR-ByteHrs', 'CAN1-TimedStorage-GIR-ByteHrs', 
                            'EU-TimedStorage-GIR-ByteHrs', 'EUC1-TimedStorage-GIR-ByteHrs', 
                            'EUN1-TimedStorage-GIR-ByteHrs', 'EUW2-TimedStorage-GIR-ByteHrs', 
                            'EUW3-TimedStorage-GIR-ByteHrs', 'SAE1-TimedStorage-GIR-ByteHrs', 
                            'TimedStorage-GIR-ByteHrs', 'TimedStorage-GIR-SmObjects', 
                            'UGW1-TimedStorage-GIR-ByteHrs', 'USE2-TimedStorage-GIR-ByteHrs', 
                            'USE2-TimedStorage-GIR-SmObjects', 'USW1-TimedStorage-GIR-ByteHrs', 
                            'USW2-TimedStorage-GIR-ByteHrs'
                        ]
                    }
                },
                {
                    'Not': {
                        'Dimensions': {
                            'Key': 'RECORD_TYPE',
                            'Values': ['Credit', 'Refund']
                        }
                    }
                },
                {
                    'Dimensions': {
                        'Key': 'OPERATION',
                        'Values': ['GlacierInstantRetrievalStorage']
                    }
                }
            ]
        }
    )

    # Parse the response to get storage usage values
    results = response.get('ResultsByTime', [])
    if not results:
        return

    for result in results:
        groups = result.get('Groups', [])
        if not groups:
            continue
        for group in groups:
            metrics = group.get('Metrics', {})
            usage_quantity = metrics.get('UsageQuantity', {}).get('Amount', '0')
            if float(usage_quantity) > 0:
                print(f"S3 GIR: {usage_quantity} GB")
    return usage_quantity

def get_cost_public():
    # Initialize a session using Amazon Cost Explorer with a specific profile
    session = boto3.Session(profile_name='payer')
    client = session.client('ce')

    # Calculate the date range
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    first_day_of_previous_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)

    start_date = first_day_of_previous_month.strftime('%Y-%m-%d')
    end_date = first_day_of_current_month.strftime('%Y-%m-%d')

    # Create a cost explorer query with filters
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=['NetAmortizedCost'],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
        ],
        Filter={
            'And': [
                {
                    'Dimensions': {
                        'Key': 'LINKED_ACCOUNT',
                        'Values': ['480094393194', '020502556137', '257519804744']
                    }
                },
                {
                    'Not': {
                        'Dimensions': {
                            'Key': 'REGION',
                            'Values': ['us-gov-west-1']
                        }
                    }
                },
                {
                    'Not': {
                        'Dimensions': {
                            'Key': 'RECORD_TYPE',
                            'Values': ['Credit', 'Refund', 'Support_fee', 'Upfront_reservation_fee']
                        }
                    }
                }
            ]
        }
    )

    # Parse the response to get costs
    results = response.get('ResultsByTime', [])
    if not results:
        return

    total_cost = 0.0
    s3_cost = 0.0
    dynamodb_cost = 0.0
    ec2_cost = 0.0

    for result in results:
        groups = result.get('Groups', [])
        for group in groups:
            service = group.get('Keys', [])[0]
            amount = group.get('Metrics', {}).get('NetAmortizedCost', {}).get('Amount', '0')
            amount = float(amount)
            if not service == 'AWS Support (Enterprise)':
                total_cost += amount
            if service == 'Amazon Simple Storage Service':
                s3_cost += amount
            elif service == 'Amazon DynamoDB':
                dynamodb_cost += amount
            elif service == 'Amazon Elastic Compute Cloud - Compute':
                ec2_cost += amount

    print(f"AWS Cost: ${total_cost:.2f}")
    print(f"S3 Cost Bill: ${s3_cost:.2f}")
    print(f"DynamoDb Cost: ${dynamodb_cost:.2f}")
    print(f"EC2 Cost: ${ec2_cost:.2f}")
    return total_cost, s3_cost, dynamodb_cost, ec2_cost

def get_costs_gov():
    # Initialize a session using Amazon Cost Explorer with a specific profile
    session = boto3.Session(profile_name='payer')
    client = session.client('ce')

    # Calculate the date range
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    first_day_of_previous_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)

    start_date = first_day_of_previous_month.strftime('%Y-%m-%d')
    end_date = first_day_of_current_month.strftime('%Y-%m-%d')

    # Create a cost explorer query with filters
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=['NetAmortizedCost'],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
        ],
        Filter={
            'And': [
                {
                    'Dimensions': {
                        'Key': 'LINKED_ACCOUNT',
                        'Values': ['480094393194']
                    }
                },
                {
                    'Dimensions': {
                        'Key': 'REGION',
                        'Values': ['us-gov-west-1']
                    }
                },
                {
                    'Not': {
                        'Dimensions': {
                            'Key': 'RECORD_TYPE',
                            'Values': ['Credit', 'Refund']
                        }
                    }
                }
            ]
        }
    )

    # Parse the response to get costs
    results = response.get('ResultsByTime', [])
    if not results:
        return

    total_cost = 0.0
    s3_cost = 0.0
    dynamodb_cost = 0.0
    ec2_cost = 0.0
    rds_cost = 0.0

    for result in results:
        groups = result.get('Groups', [])
        for group in groups:
            service = group.get('Keys', [])[0]
            amount = group.get('Metrics', {}).get('NetAmortizedCost', {}).get('Amount', '0')
            amount = float(amount)
            total_cost += amount
            if service == 'Amazon Simple Storage Service':
                s3_cost += amount
            elif service == 'Amazon DynamoDB':
                dynamodb_cost += amount
            elif service == 'Amazon Elastic Compute Cloud - Compute':
                ec2_cost += amount
            elif service == 'Amazon Relational Database Service':
                rds_cost += amount

    print(f"Total: ${total_cost:.2f}")
    print(f"S3: ${s3_cost:.2f}")
    print(f"DynamoDb: ${dynamodb_cost:.2f}")
    print(f"EC2: ${ec2_cost:.2f}")
    print(f"RDS: ${rds_cost:.2f}")
    return total_cost, s3_cost, dynamodb_cost, ec2_cost, rds_cost

def get_creation_date_from_filename(filename):
    """Extract creation date from the filename."""
    date_match = re.search(r'_(\d{4}-\d{2}-\d{2})\.csv$', filename)
    if date_match:
        return date_match.group(1)
    return None

def apply_filters_and_calculate_counts(file_path, filename):
    """Apply filters to the CSV file and calculate various counts."""
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Apply filters for Paid customers
    filtered_df_paid = df[
        (df['Customer State'].isin(['Expiring soon', 'Active', 'Created'])) &
        (df['Customer Type'] == 'Paid')
    ]

    # Apply filters for Eval customers
    filtered_df_eval = df[
        (df['Customer State'].isin(['Expiring soon', 'Active', 'Created'])) &
        (df['Customer Type'] == 'Eval')
    ]

    # Ensure the columns are numeric
    filtered_df_paid['Licensed Users'] = pd.to_numeric(filtered_df_paid['Licensed Users'], errors='coerce').fillna(0)
    filtered_df_paid['Users with at least 1 device'] = pd.to_numeric(filtered_df_paid['Users with at least 1 device'], errors='coerce').fillna(0)
    filtered_df_paid['Storage_Used_In_GB'] = pd.to_numeric(filtered_df_paid['Storage_Used_In_GB'], errors='coerce').fillna(0)
    filtered_df_paid['S3_Usage_In_GB'] = pd.to_numeric(filtered_df_paid['S3_Usage_In_GB'], errors='coerce').fillna(0)

    # Calculate various counts for Paid customers
    paid_count = filtered_df_paid.shape[0]
    licensed_users = filtered_df_paid['Licensed Users'].sum()
    active_users = filtered_df_paid['Users with at least 1 device'].sum()
    used_storage_in_gb = filtered_df_paid['Storage_Used_In_GB'].sum()
    s3_usage_in_gb = filtered_df_paid['S3_Usage_In_GB'].sum()

    # Calculate count for Eval customers
    eval_count = filtered_df_eval.shape[0]

    # Extract creation date from the filename
    creation_date = get_creation_date_from_filename(filename)

    return {
        'eval_count': eval_count,
        'paid_count': paid_count,
        'licensed_users': licensed_users,
        'active_users': active_users,
        'used_storage_in_gb': used_storage_in_gb,
        's3_usage_in_gb': s3_usage_in_gb,
        'creation_date': creation_date
    }

def filter_usage_data_public():
    folder_path = 'attachments'
    files = os.listdir(folder_path)

    total_eval_count = 0
    total_paid_count = 0
    total_licensed_users = 0
    total_active_users = 0
    
def filter_usage_data_public():
    folder_path = 'attachments'
    files = os.listdir(folder_path)

    total_eval_count = 0
    total_paid_count = 0
    total_licensed_users = 0
    total_active_users = 0
    total_used_storage_in_gb = 0
    total_s3_usage_in_gb = 0

    for file_name in files:
        if 'us0' in file_name.lower() or 'ap1' in file_name.lower():
            file_path = os.path.join(folder_path, file_name)

            # Check if the file exists
            if os.path.exists(file_path):
                counts = apply_filters_and_calculate_counts(file_path, file_name)
                total_eval_count += counts['eval_count']
                total_paid_count += counts['paid_count']
                total_licensed_users += counts['licensed_users']
                total_active_users += counts['active_users']
                total_used_storage_in_gb += counts['used_storage_in_gb']
                total_s3_usage_in_gb += counts['s3_usage_in_gb']

                # Print counts for individual file
                print(f"File: {file_name}, Eval Count: {counts['eval_count']}, Paid Count: {counts['paid_count']}, Licensed Users: {counts['licensed_users']}, "
                      f"Active Users: {counts['active_users']}, Used Storage in GB: {counts['used_storage_in_gb']}, "
                      f"S3 Usage in GB: {counts['s3_usage_in_gb']}, Creation Date: {counts['creation_date']}")
            else:
                print(f'File not found: {file_path}')

    # Print total counts
    print(f'Total Eval Count: {total_eval_count}')
    print(f'Total Paid Count: {total_paid_count}')
    print(f'Total Licensed Users: {total_licensed_users}')
    print(f'Total Active Users: {total_active_users}')
    print(f'Total Used Storage in GB: {total_used_storage_in_gb}')
    print(f'Total S3 Usage in GB: {total_s3_usage_in_gb}')
    return total_eval_count, total_paid_count, total_licensed_users, total_active_users, total_used_storage_in_gb, total_s3_usage_in_gb

def filter_usage_data_gov():
    folder_path = 'attachments'
    files = os.listdir(folder_path)

    total_eval_count = 0
    total_paid_count = 0
    total_licensed_users = 0
    total_active_users = 0
    total_used_storage_in_gb = 0
    total_s3_usage_in_gb = 0

    for file_name in files:
        if 'gov' in file_name.lower():
            file_path = os.path.join(folder_path, file_name)

            # Check if the file exists
            if os.path.exists(file_path):
                counts = apply_filters_and_calculate_counts(file_path, file_name)
                total_eval_count += counts['eval_count']
                total_paid_count += counts['paid_count']
                total_licensed_users += counts['licensed_users']
                total_active_users += counts['active_users']
                total_used_storage_in_gb += counts['used_storage_in_gb']
                total_s3_usage_in_gb += counts['s3_usage_in_gb']

                # Print counts for individual file
                print(f"File: {file_name}, Eval Count: {counts['eval_count']}, Paid Count: {counts['paid_count']}, Licensed Users: {counts['licensed_users']}, "
                      f"Active Users: {counts['active_users']}, Used Storage in GB: {counts['used_storage_in_gb']}, "
                      f"S3 Usage in GB: {counts['s3_usage_in_gb']}, Creation Date: {counts['creation_date']}")
            else:
                print(f'File not found: {file_path}')

    # Print total counts
    print(f'Total Eval Count: {total_eval_count}')
    print(f'Total Paid Count: {total_paid_count}')
    print(f'Total Licensed Users: {total_licensed_users}')
    print(f'Total Active Users: {total_active_users}')
    print(f'Total Used Storage in GB: {total_used_storage_in_gb}')
    print(f'Total S3 Usage in GB: {total_s3_usage_in_gb}')
    return total_eval_count, total_paid_count, total_licensed_users, total_active_users, total_used_storage_in_gb, total_s3_usage_in_gb

def authenticate_google_sheets():
    
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    
    return build("sheets", "v4", credentials=creds)

def get_last_row(service,sheet_name):
    """Get the last row with data in the sheet."""
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{sheet_name}!A:A").execute()
    values = result.get("values", [])

    return len(values)  # Returns the last non-empty row index

def copy_row(service, sheet_id, source_row):
    """Copy the last row with formulas and formatting to the next row."""
    sheet = service.spreadsheets()
    destination_row = source_row + 1

    copy_request = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": source_row - 1,  # 0-based index
                        "endRowIndex": source_row
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": destination_row - 1,
                        "endRowIndex": destination_row
                    },
                    "pasteType": "PASTE_NORMAL",
                    "pasteOrientation": "NORMAL"
                }
            }
        ]
    }

    sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=copy_request).execute()
    print(f"Row {source_row} copied to row {destination_row} successfully.")


def update_values(service, row_number, column_values, sheet_name):
    """Update specific column values in the newly copied row."""
    sheet = service.spreadsheets()
    update_data = []

    # Convert numpy int64 to Python int
    column_values = {k: int(v) if isinstance(v, np.int64) else v for k, v in column_values.items()}

    for col, value in column_values.items():
        print(value)
        
        update_data.append({
            "range": f"{sheet_name}!{col}{row_number}",
            "values": [[value]]
        })

    if update_data:
        print("======================================")
        print(update_data)
        print("======================================")
        sheet.values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": update_data}
        ).execute()
        print(f"Updated values in row {row_number}.")


def get_sheet_id(service, spreadsheet_id, sheet_name):
    """Retrieve the sheet ID for the given sheet name."""
    sheets_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheets_metadata.get('sheets', [])
    for sheet in sheets:
        if sheet.get("properties", {}).get("title") == sheet_name:
            return sheet.get("properties", {}).get("sheetId")
    return None

def column_index_to_letter(index):
    """Convert a column index into a column letter (e.g., 0 -> A, 27 -> AB)."""
    string = ""
    while index >= 0:
        string = chr(index % 26 + 65) + string
        index = index // 26 - 1
    return string

def copy_last_column_and_update_values(service, column_values):
    """Copy the last column from rows 23 to 42 and update values column-wise."""
    sheet = service.spreadsheets()
    
    # Determine the last column with data in the specified range
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_GOV}!23:43").execute()
    values = result.get("values", [])
    last_column_index = len(values[0]) - 1

    # Define the range for the source and destination columns
    source_range = f"{SHEET_GOV}!{column_index_to_letter(last_column_index)}23:{column_index_to_letter(last_column_index)}43"
    destination_column = column_index_to_letter(last_column_index + 1)
    destination_range = f"{SHEET_GOV}!{destination_column}23:{destination_column}43"

    # Get the sheet ID
    sheet_id = get_sheet_id(service, SPREADSHEET_ID, SHEET_GOV)

    # Copy the last column to the new column
    copy_request = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": 22,  # Row 23 (0-based index)
                        "endRowIndex": 43,    # Row 44 (0-based index)
                        "startColumnIndex": last_column_index,
                        "endColumnIndex": last_column_index + 1
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": 22,  # Row 23 (0-based index)
                        "endRowIndex": 43,    # Row 44 (0-based index)
                        "startColumnIndex": last_column_index + 1,
                        "endColumnIndex": last_column_index + 2
                    },
                    "pasteType": "PASTE_NORMAL",
                    "pasteOrientation": "NORMAL"
                }
            }
        ]
    }

    sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=copy_request).execute()
    print(f"Copied column {chr(65 + last_column_index)} to {destination_column} successfully.")

    # Convert numpy int64 to Python int
    column_values = {k: int(v) if isinstance(v, np.int64) else v for k, v in column_values.items()}

    # Update the values in the new column
    update_data = []
    
    # Mapping of rows to values
    row_value_mapping = {
        23: column_values['date'],
        25: column_values['dynamodb_cost_gov'],
        26: column_values['ec2_cost_gov'],
        27: column_values['rds_cost_gov'],
        28: column_values['s3_cost_gov'],
        32: column_values['total_cost_gov'],
        33: column_values['total_licensed_users_gov'],
        34: column_values['total_active_users_gov'],
        35: column_values['total_used_storage_in_gb_gov'],
        36: column_values['total_s3_usage_in_gb_gov']
    }
    
    for row, value in row_value_mapping.items():
        update_data.append({
            "range": f"{SHEET_GOV}!{destination_column}{row}",
            "values": [[value]]
        })

    if update_data:
        sheet.values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": update_data}
        ).execute()
        print(f"Updated values in column {destination_column} for specified rows.")




def phoenix(service, last_month_date):
    """Copy the last columnlast_month_date from rows 64 to 110 in Main Sheet and update values from AWS Cost Explorer."""
    sheet = service.spreadsheets()
    
    # Determine the last column with data in the specified range
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_GOV}!64:111").execute()
    values = result.get("values", [])
    if not values:
        print("No data found in the specified range.")
        return

    last_column_index = len(values[0]) - 1 if values[0] else 0
    print(f"Last column index with data: {last_column_index}")
    
    # Ensure the spreadsheet has enough columns
    required_columns = last_column_index + 2  # Current last column + new column
    grid_properties = sheet.get(spreadsheetId=SPREADSHEET_ID).execute().get('sheets')[0].get('properties').get('gridProperties')
    current_column_count = grid_properties.get('columnCount')
    print(f"Current column count: {current_column_count}")

    if current_column_count < required_columns:
        add_columns_request = {
            "requests": [
                {
                    "appendDimension": {
                        "sheetId": get_sheet_id(service, SPREADSHEET_ID, SHEET_GOV),
                        "dimension": "COLUMNS",
                        "length": required_columns - current_column_count
                    }
                }
            ]
        }
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=add_columns_request).execute()
        print(f"Added {required_columns - current_column_count} columns to the sheet.")

    # Define the range for the source and destination columns
    source_column_letter = column_index_to_letter(last_column_index)
    destination_column_letter = column_index_to_letter(last_column_index + 1)
    source_range = f"{SHEET_GOV}!{source_column_letter}64:{source_column_letter}110"
    destination_range = f"{SHEET_GOV}!{destination_column_letter}64:{destination_column_letter}110"
    print(f"Source range: {source_range}")
    print(f"Destination range: {destination_range}")

    # Get the sheet ID
    sheet_id = get_sheet_id(service, SPREADSHEET_ID, SHEET_GOV)

    # Copy the last column to the new column
    copy_request = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": 63,  # Row 64 (0-based index)
                        "endRowIndex": 110,   # Row 111 (0-based index)
                        "startColumnIndex": last_column_index,
                        "endColumnIndex": last_column_index + 1
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": 63,  # Row 64 (0-based index)
                        "endRowIndex": 110,   # Row 111 (0-based index)
                        "startColumnIndex": last_column_index + 1,
                        "endColumnIndex": last_column_index + 2
                    },
                    "pasteType": "PASTE_NORMAL",
                    "pasteOrientation": "NORMAL"
                }
            }
        ]
    }

    sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=copy_request).execute()
    print(f"Copied column {source_column_letter} to {destination_column_letter} successfully.")

    # Fetch values from AWS Cost Explorer
    total_cost1, dynamodb_cost1, ec2_cost1, rds_cost1, s3_cost1 = fetch_aws_costs("697280920917")
    total_cost2, dynamodb_cost2, ec2_cost2, rds_cost2, s3_cost2 = fetch_aws_costs("361870911536")

    # Update the values in the new column
    update_data = [
        {"range": f"{SHEET_GOV}!{destination_column_letter}64", "values": [[last_month_date]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}66", "values": [[dynamodb_cost1]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}67", "values": [[ec2_cost1]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}68", "values": [[rds_cost1]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}69", "values": [[s3_cost1]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}74", "values": [[total_cost1]]},

        {"range": f"{SHEET_GOV}!{destination_column_letter}81", "values": [[last_month_date]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}83", "values": [[dynamodb_cost2]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}84", "values": [[ec2_cost2]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}85", "values": [[rds_cost2]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}86", "values": [[s3_cost2]]},
        {"range": f"{SHEET_GOV}!{destination_column_letter}91", "values": [[total_cost2]]}
    ]

    if update_data:
        sheet.values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": update_data}
        ).execute()
        print(f"Updated values in column {destination_column_letter} for specified rows.")

def fetch_aws_costs(linked_account):
    # Initialize a session using Amazon Cost Explorer with a specific profile
    session = boto3.Session(profile_name='payer')
    client = session.client('ce')

    # Calculate the date range
    start_date = "2025-02-01"
    end_date = "2025-03-01"

    # Create a cost explorer query with filters
    response = client.get_cost_and_usage(
        TimePeriod={'Start': start_date, 'End': end_date},
        Granularity='MONTHLY',
        Metrics=['NetAmortizedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}],
        Filter={
            'And': [
                {'Dimensions': {'Key': 'LINKED_ACCOUNT', 'Values': [linked_account]}},
                {'Not': {'Dimensions': {'Key': 'RECORD_TYPE', 'Values': ['Credit', 'Refund', 'Enterprise Discount Program Discount']}}}
            ]
        }
    )

    # Initialize cost variables
    total_cost = 0.0
    dynamodb_cost = 0.0
    ec2_cost = 0.0
    rds_cost = 0.0
    s3_cost = 0.0

    # Parse the response to get costs
    results = response.get('ResultsByTime', [])
    if not results:
        return total_cost, dynamodb_cost, ec2_cost, rds_cost, s3_cost

    for result in results:
        groups = result.get('Groups', [])
        for group in groups:
            service = group.get('Keys', [])[0]
            amount = float(group.get('Metrics', {}).get('NetAmortizedCost', {}).get('Amount', '0'))
            total_cost += amount
            if service == 'Amazon DynamoDB':
                dynamodb_cost += amount
            elif service == 'Amazon Elastic Compute Cloud - Compute':
                ec2_cost += amount
            elif service == 'Amazon Relational Database Service':
                rds_cost += amount
            elif service == 'Amazon Simple Storage Service':
                s3_cost += amount

    return total_cost, dynamodb_cost, ec2_cost, rds_cost, s3_cost




def main():
    service = authenticate_gmail()
    now = datetime.now()
    first_of_month = datetime(now.year, now.month, 1)
    start_date = (first_of_month - timedelta(days=7)).strftime('%Y/%m/%d')
    end_date = (first_of_month + timedelta(days=7)).strftime('%Y/%m/%d')
    
    subjects = ["inSyncCloud Weekly Report", "AP1_inSyncCloud Weekly Report", "inSyncGovCloud Weekly Report on"]
    folder_path = 'attachments'
    os.makedirs(folder_path, exist_ok=True)
    
    for subject in subjects:
        query = f'subject:("{subject}") after:{start_date} before:{end_date}'
        messages = search_emails(service, query)
        if not messages:
            print(f'No messages found for subject: {subject}')
            continue
        
        closest_message = None
        min_diff = timedelta(days=8)  # Initialize with a value larger than 7 days
        
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id']).execute()
            email_subject = ''
            for header in msg['payload']['headers']:
                if header['name'] == 'Subject':
                    email_subject = header['value']
                    print(f'Processing email with subject: {email_subject}')
                    break
            
            email_date = extract_date_from_subject(email_subject)
            if email_date:
                diff = abs(email_date - first_of_month)
                if diff < min_diff:
                    min_diff = diff
                    closest_message = msg
        
        if closest_message:
            email_subject = ''
            for header in closest_message['payload']['headers']:
                if header['name'] == 'Subject':
                    email_subject = header['value']
                    print(email_subject)
                    break
            download_attachments(service, closest_message['id'], folder_path, email_subject)
    s3_as_per_ce = get_previous_month_storage_usage()
    gir_as_per_ce = get_previous_month_s3_gir_usage()
    total_cost_public, s3_cost_public, dynamodb_cost_public, ec2_cost_public = get_cost_public()
    total_cost_gov, s3_cost_gov, dynamodb_cost_gov, ec2_cost_gov, rds_cost_gov = get_costs_gov()

    total_eval_count, total_paid_count, total_licensed_users, total_active_users, total_s3_usage_in_gb, total_used_storage_in_gb = filter_usage_data_public()
    
    total_eval_count_gov, total_paid_count_gov, total_licensed_users_gov, total_active_users_gov, total_s3_usage_in_gb_gov, total_used_storage_in_gb_gov = filter_usage_data_gov()
    
    service = authenticate_google_sheets()
    sheet_id = get_sheet_id(service,SPREADSHEET_ID, SHEET_NAME)
    last_row = get_last_row(service,SHEET_NAME)
    print(f"Last row with data: {last_row}")

    copy_row(service, sheet_id, last_row)

    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    
    formatted_date = first_day_of_previous_month.strftime('%m/%Y')
    last_month_date = last_day_of_previous_month.strftime('%m/%d/%Y')
    todays_date = today.strftime('%m/%d/%Y')

    # Define column updates (Example: Change column B and C in the new row)
    new_values = {
        "A": formatted_date,
        "B": total_licensed_users,
        "c": total_active_users,
        "D": total_s3_usage_in_gb,
        "E": total_used_storage_in_gb,
        "F": s3_as_per_ce,
        "G": gir_as_per_ce,
        "H": total_cost_public,
        "I": s3_cost_public,
        "J": dynamodb_cost_public,
        "K": ec2_cost_public,
        "N": total_eval_count,
        "O": total_paid_count,
    }
    
    update_values(service, last_row + 1, new_values, SHEET_NAME)

    column_values = {
        'date': last_month_date,
        'total_cost_gov': total_cost_gov,
        's3_cost_gov': s3_cost_gov,
        'dynamodb_cost_gov': dynamodb_cost_gov,
        'ec2_cost_gov': ec2_cost_gov,
        'rds_cost_gov': rds_cost_gov,
        'total_licensed_users_gov': total_licensed_users_gov,
        'total_active_users_gov': total_active_users_gov,
        'total_used_storage_in_gb_gov': total_used_storage_in_gb_gov,
        'total_s3_usage_in_gb_gov': total_s3_usage_in_gb_gov
    }
    
    copy_last_column_and_update_values(service, column_values)


    sheet_id = get_sheet_id(service,SPREADSHEET_ID, SHEET_FEDRAMP)
    last_row = get_last_row(service, SHEET_FEDRAMP)
    copy_row(service, sheet_id, last_row)
    new_values = {
        "A": last_month_date,
        "CV": todays_date,
        "CW": total_licensed_users_gov,
        "CX": total_active_users_gov,
        "CY": total_used_storage_in_gb_gov,
        "CZ": total_s3_usage_in_gb_gov,
        "DC": total_cost_gov,
        "DD": s3_cost_gov,
        "DE": dynamodb_cost_gov,
        "DF": ec2_cost_gov,
        "DI": total_eval_count_gov,
        "DJ": total_paid_count_gov,
    }


    update_values(service, last_row + 1, new_values, SHEET_FEDRAMP)

    phoenix(service, last_month_date)


if __name__ == '__main__':
    main()
