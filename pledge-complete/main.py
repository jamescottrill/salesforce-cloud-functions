from simple_salesforce import Salesforce, exceptions
import os
from google.cloud import secretmanager
import base64
from datetime import datetime
from distutils import util
# from dotenv import load_dotenv

secret_client = secretmanager.SecretManagerServiceClient()


# load_dotenv()


def get_secret(project_id, secret_id, version_id):
    # Build the resource name of the secret version.
    name = secret_client.secret_version_path(project_id, secret_id, version_id)

    # Access the secret version.
    response = secret_client.access_secret_version(name)

    # Decode the payload
    payload = response.payload.data.decode('UTF-8')

    # Return the secret
    return payload


email = os.environ.get('email')
email_dev = os.environ.get('email_dev')
password = get_secret("PROJECT", "KEY", int(os.environ.get('password_version')))
environment = os.environ.get('environment')
token = get_secret("PROJECT", "KEY", int(os.environ.get('token_version')))
token_dev = get_secret("PROJECT", "KEY", int(os.environ.get('token_dev_version')))

sf_live = Salesforce(username=email, password=password, security_token=token)
try:
    sf_dev = Salesforce(username=email_dev, password=password, security_token=token_dev, domain=environment)
except exceptions.SalesforceAuthenticationFailed:
    sf_dev = sf_live
sf = None


# noinspection PyShadowingNames
def run(event: dict, context: dict):
    global sf
    data = eval(base64.b64decode(event['data']))
    sf = sf_live if data['url'] == ('protectourwinters.uk' or 'pledge.protectourwinters.uk') else sf_dev
    today = datetime.now().strftime("%Y-%m-%d")
    update = {
        'The_Pledge_Start_Date__c': today,
        'The_Pledge_End_Date__c': data['expiry_date'],
        'Pledge_Origin__c': 'New',
        'The_Pledge_Level__c': data['pledge_level']
    }
    res = sf.Opportunity.update(data['opportunity_id'], update)
    if res == 204:
        update = {
            'CloseDate': today,
            'Amount': data['pledge_cost'],
            'StageName': 'Closed Won',
            'npe01__Do_Not_Automatically_Create_Payment__c': bool(util.strtobool(data['invoiced']))
        }
        sf.Opportunity.update(data['opportunity_id'], update)
