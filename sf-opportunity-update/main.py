from simple_salesforce import Salesforce, exceptions
import os
from google.cloud import secretmanager
import base64

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
password = get_secret("pow-uk-website", "jc_salesforce_password", int(os.environ.get('password_version')))
environment = os.environ.get('environment')
token = get_secret("136763623377", "jc_salesforce_token", int(os.environ.get('token_version')))
token_dev = get_secret("136763623377", "jc_salesforce_token_dev", int(os.environ.get('token_dev_version')))

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
    sf = sf_live if data['url'] == ("protectourwinters.uk" or "pledge.protectourwinters.uk") else sf_dev
    res = sf.Opportunity.update(data['opportunity_id'], {"StageName": data['opportunity_stage']})
