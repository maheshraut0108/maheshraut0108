import os
import subprocess
import json
import google.auth
import google.auth.transport.requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from datetime import datetime

# Google Sheet details
SOURCE_SPREADSHEET_ID = '1qL3zf2haBWfB2weYeWCCJF_354oClZH2F-C_IVC-BQM'

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


def authenticate():
    """Authenticate the user using service account and return the service"""
    creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    return creds

def copy_and_rename_sheet(service, source_spreadsheet_id, new_sheet_name):
    """Make a copy of the source spreadsheet and rename it"""
    try:
        # Make a copy of the spreadsheet
        copy_sheet = service.files().copy(fileId=source_spreadsheet_id, body={'name': new_sheet_name}).execute()
        return copy_sheet['id']
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

def update_sheet_values(service, spreadsheet_id, values):
    """Update the specified values in the spreadsheet"""
    try:
        data = [{'range': key, 'values': [[value]]} for key, value in values.items()]
        body = {'valueInputOption': 'RAW', 'data': data}
        result = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        print(f"{result.get('totalUpdatedCells')} cells updated.")
    except HttpError as error:
        print(f"An error occurred: {error}")

def run_query(command):
    """Run a shell command and return the output"""
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        print(f"Error running command: {stderr.decode('utf-8')}")
        return None
    return stdout.decode('utf-8').strip()

def get_values_from_aliases():
    """Run the SQL queries using aliases and parse the output"""
    queries = {
        'Total_Tenants': [
            "insync_rds -e \"SELECT COUNT(customer.id) AS Total_Tenants FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;\"",
            "insync_rds_prod_D1 -e \"SELECT COUNT(customer.id) AS Total_Tenants FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;\""
        ],
        'Total_Lic_Users': [
            "insync_rds -e \"SELECT SUM(customer_license.total_m365_users) AS Total_Lic_Users FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;\"",
            "insync_rds_prod_D1 -e \"SELECT SUM(customer_license.total_m365_users) AS Total_Lic_Users FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;\""
        ],
        'Total_Active_Users': [
            "insync_rds -e \"SELECT COUNT(product_ca_consumption.id) AS Active_Users FROM customer, customer_license, product_ca_consumption WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;\"",
            "insync_rds_prod_D1 -e \"SELECT COUNT(product_ca_consumption.id) AS Active_Users FROM customer, customer_license, product_ca_consumption WHERE customer.id = customer_license.customerid AND customer_license.m365_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_m365_users > 0;\""
        ],
        'Active_DevicesFS': [
            "insync_rds -e \"SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM (SELECT COUNT(IF(device.device_disabled = 0, 1, NULL)) AS Active_Devices, SUM(device.fstotaluGB) AS fstotaluGB FROM device WHERE device_disabled = 0) AS Active_DevicesFS;\"",
            "insync_rds_prod_D1 -e \"SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM (SELECT COUNT(IF(device.device_disabled = 0, 1, NULL)) AS Active_Devices, SUM(device.fstotaluGB) AS fstotaluGB FROM device WHERE device_disabled = 0) AS Active_DevicesFS;\""
        ],
        'Active_DevicesOD': [
            "insync_rds -e \"SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM (SELECT COUNT(IF(device.device_disabled = 0, 1, NULL)) AS Active_Devices, SUM(device.fstotaluGB) AS fstotaluGB FROM device WHERE device_disabled = 0) AS Active_DevicesOD;\"",
            "insync_rds_prod_D1 -e \"SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM (SELECT COUNT(IF(device.device_disabled = 0, 1, NULL)) AS Active_Devices, SUM(device.fstotaluGB) AS fstotaluGB FROM device WHERE device_disabled = 0) AS Active_DevicesOD;\""
        ],
        'Active_Sites': [
            "isharepoint_prod -e \"SELECT COUNT(IF(share_point_site_collection.configured = 1, 1, NULL)) AS Active_Sites, SUM(fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM share_point_site_collection;\"",
            "insync_sharepoint_rds_d1_prod -e \"SELECT COUNT(IF(share_point_site_collection.configured = 1, 1, NULL)) AS Active_Sites, SUM(fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM share_point_site_collection;\""
        ],
        'Active_Teams': [
            "iMSGroupRDS_rds -e \"SELECT COUNT(IF(cloud_msteam.team.configured = 1, 1, NULL)) AS Active_Teams, (SUM(cloud_msteam.team.fstotalu) + SUM(cloud_msteam.conversation_stat.fstotalu)) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_msteam;\"",
            "msgrouprds-prod-D1 -e \"SELECT COUNT(IF(cloud_1_msteam.team.configured = 1, 1, NULL)) AS Active_Teams, (SUM(cloud_1_msteam.team.fstotalu) + SUM(cloud_1_msteam.conversation_stat.fstotalu)) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_1_msteam;\""
        ],
        'Active_groups': [
            "iMSGroupRDS_rds -e \"SELECT COUNT(id) FROM cloud_msgroup.groups_table WHERE configured = 1;\"",
            "msgrouprds-prod-D1 -e \"SELECT COUNT(id) FROM cloud_1_msgroup.groups_table WHERE configured = 1;\""
        ],
        'Active_Folders': [
            "capp_rds -e \"SELECT COUNT(IF(cloud_pf.public_folder.configured = 1, 1, NULL)) AS Active_Folders, SUM(cloud_pf.public_folder.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_pf;\"",
            "insync_sharepoint_rds_d1_prod -e \"SELECT COUNT(IF(cloud_1_pf.public_folder.configured = 1, 1, NULL)) AS Active_Folders, SUM(cloud_1_pf.public_folder.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_1_pf;\""
        ],
        'GSuite_Total_Tenants': [
            "insync_rds -e \"SELECT COUNT(customer.id) AS Total_Tenants FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;\"",
            "insync_rds_prod_D1 -e \"SELECT COUNT(customer.id) AS Total_Tenants FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;\""
        ],
        'GSuite_Total_Lic_Users': [
            "insync_rds -e \"SELECT SUM(customer_license.total_google_users) AS Total_Lic_Users FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;\"",
            "insync_rds_prod_D1 -e \"SELECT SUM(customer_license.total_google_users) AS Total_Lic_Users FROM customer, customer_license WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;\""
        ],
        'GSuite_Total_Active_Users': [
            "insync_rds -e \"SELECT COUNT(product_ca_consumption.id) AS Active_Users FROM customer, customer_license, product_ca_consumption WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;\"",
            "insync_rds_prod_D1 -e \"SELECT COUNT(product_ca_consumption.id) AS Active_Users FROM customer, customer_license, product_ca_consumption WHERE customer.id = customer_license.customerid AND customer_license.google_license_expiry > (SELECT UNIX_TIMESTAMP(NOW())) AND customer_license.type = 'Commercial' AND customer_license.total_google_users > 0;\""
        ],
        'GSuite_Active_Devices': [
            "insync_rds -e \"SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM (SELECT COUNT(IF(device.device_disabled = 0, 1, NULL)) AS Active_Devices, SUM(device.fstotaluGB) AS fstotaluGB FROM device WHERE device_disabled = 0) AS Active_Devices;\"",
            "insync_rds_prod_D1 -e \"SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM (SELECT COUNT(IF(device.device_disabled = 0, 1, NULL)) AS Active_Devices, SUM(device.fstotaluGB) AS fstotaluGB FROM device WHERE device_disabled = 0) AS Active_Devices;\""
        ],
        'GDrive_Active_Devices': [
            "insync_rds -e \"SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM (SELECT COUNT(IF(device.device_disabled = 0, 1, NULL)) AS Active_Devices, SUM(device.fstotaluGB) AS fstotaluGB FROM device WHERE device_disabled = 0) AS Active_Devices;\"",
            "insync_rds_prod_D1 -e \"SELECT SUM(Active_Devices) AS Active_Devices, SUM(fstotaluGB) AS FS_TOTAL_U FROM (SELECT COUNT(IF(device.device_disabled = 0, 1, NULL)) AS Active_Devices, SUM(device.fstotaluGB) AS fstotaluGB FROM device WHERE device_disabled = 0) AS Active_Devices;\""
        ],
        'Gsuite_Shared_Active_Drives': [
            "capp_rds -e \"SELECT COUNT(IF(cloud_gteamdrive.team_drives_collection.configured = 1, 1, NULL)) AS Active_Drives, SUM(cloud_gteamdrive.team_drives_collection.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_gteamdrive;\"",
            "insync_sharepoint_rds_d1_prod -e \"SELECT COUNT(IF(cloud_1_gteamdrive.team_drives_collection.configured = 1, 1, NULL)) AS Active_Drives, SUM(cloud_1_gteamdrive.team_drives_collection.fstotalu) / (1024 * 1024 * 1024) AS fstotaluGB FROM cloud_1_gteamdrive;\""
        ],
        'Endpoint_Active_Tenants': [
            "insync_rds -e \"select count(*) from customer_license where total_device_users != 0 and from_unixtime(timelimit) > NOW();\"",
            "insync_rds_prod_D1 -e \"select count(*) from customer_license where total_device_users != 0 and from_unixtime(timelimit) > NOW();\""
        ],
        'Endpoint_Total_Lic_Users': [
            "insync_rds -e \"select sum(total_device_users) from customer_license where total_device_users != 0 and from_unixtime(timelimit) > NOW();\"",
            "insync_rds_prod_D1 -e \"select sum(total_device_users) from customer_license where total_device_users != 0 and from_unixtime(timelimit) > NOW();\""
        ],
        'Endpoint_Total_Active_Users': [
            "insync_rds -e \"select count(distinct usertable.id) from usertable, customer_license, device where usertable.customerid = customer_license.customerid and total_device_users != 0 and from_unixtime(timelimit) > NOW();\"",
            "insync_rds_prod_D1 -e \"select count(distinct usertable.id) from usertable, customer_license, device where usertable.customerid = customer_license.customerid and total_device_users != 0 and from_unixtime(timelimit) > NOW();\""
        ],
        'Endpoint_Total_Active_Devices': [
            "insync_rds -e \"select count(distinct usertable.id) from usertable, customer_license, device where usertable.customerid = customer_license.customerid and total_device_users != 0 and from_unixtime(timelimit) > NOW();\"",
            "insync_rds_prod_D1 -e \"select count(distinct usertable.id) from usertable, customer_license, device where usertable.customerid = customer_license.customerid and total_device_users != 0 and from_unixtime(timelimit) > NOW();\""
        ],
        'Endpoint_FSTotalU': [
            "insync_rds -e \"select sum(total_data_usage)/(1024*1024*1024) from device_list_view, usertable, customer_license where total_device_users !=0 and from_unixtime(timelimit) > NOW() and device_list_view.customerid = customer_license.customerid;\"",
            "insync_rds_prod_D1 -e \"select sum(total_data_usage)/(1024*1024*1024) from device_list_view, usertable, customer_license where total_device_users !=0 and from_unixtime(timelimit) > NOW() and device_list_view.customerid = customer_license.customerid;\""
        ]
    }

    values = {}
    for key, commands in queries.items():
        value = 0
        for command in commands:
            output = run_query(command)
            if output:
                value += float(output.split()[-1])
        values[key] = value
    
    return values

def main():
    """Main function to execute the operations"""
    # Get values from database aliases
    values_from_aliases = get_values_from_aliases()

    # Prepare the values to update in the Google Sheet
    values_to_update = {
        'B4:B9': values_from_aliases['Total_Tenants'],
        'C4:C9': values_from_aliases['Total_Lic_Users'],
        'D4:D9': values_from_aliases['Total_Active_Users'],
        'E4': values_from_aliases['Active_DevicesFS'],
        'F4': values_from_aliases['Active_DevicesFS'],
        'E5': values_from_aliases['Active_DevicesOD'],
        'F5': values_from_aliases['Active_DevicesOD'],
        'E6': values_from_aliases['Active_Sites'],
        'F6': values_from_aliases['Active_Sites'],
        'E7': values_from_aliases['Active_Teams'],
        'F7': values_from_aliases['Active_Teams'],
        'E8': values_from_aliases['Active_groups'],
        'F8': values_from_aliases['Active_groups'],
        'E9': values_from_aliases['Active_Folders'],
        'F9': values_from_aliases['Active_Folders'],
        'B10:B12': values_from_aliases['GSuite_Total_Tenants'],
        'C10:C12': values_from_aliases['GSuite_Total_Lic_Users'],
        'D10:D12': values_from_aliases['GSuite_Total_Active_Users'],
        'E10': values_from_aliases['GSuite_Active_Devices'],
        'F10': values_from_aliases['GSuite_Active_Devices'],
        'E11': values_from_aliases['GDrive_Active_Devices'],
        'F11': values_from_aliases['GDrive_Active_Devices'],
        'E12': values_from_aliases['Gsuite_Shared_Active_Drives'],
        'F12': values_from_aliases['Gsuite_Shared_Active_Drives'],
        'E13': values_from_aliases['Endpoint_Active_Tenants'],
        'F13': values_from_aliases['Endpoint_Active_Tenants'],
        'E14': values_from_aliases['Endpoint_Total_Lic_Users'],
        'F14': values_from_aliases['Endpoint_Total_Lic_Users'],
        'E15': values_from_aliases['Endpoint_Total_Active_Users'],
        'F15': values_from_aliases['Endpoint_Total_Active_Users'],
        'E16': values_from_aliases['Endpoint_Total_Active_Devices'],
        'F16': values_from_aliases['Endpoint_Total_Active_Devices'],
        'E17': values_from_aliases['Endpoint_FSTotalU'],
        'F17': values_from_aliases['Endpoint_FSTotalU'],
    }

    # Authenticate and get the Google Sheets service
    creds = authenticate()
    service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    # Get current month and year
    current_date = datetime.now()
    new_sheet_name = f"CloudApps-COGS-Calculator-v1-{current_date.strftime('%B-%Y')}(US0&AP1)"

    # Step 1: Copy and rename the sheet
    copied_sheet_id = copy_and_rename_sheet(drive_service, SOURCE_SPREADSHEET_ID, new_sheet_name)
    if copied_sheet_id:
        print(f"Spreadsheet copied successfully with ID: {copied_sheet_id}")

        # Step 2: Update the values in the copied sheet
        update_sheet_values(service, copied_sheet_id, values_to_update)

