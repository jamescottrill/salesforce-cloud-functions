from simple_salesforce import Salesforce, exceptions
from google.cloud import error_reporting
from datetime import date, timedelta
import json
import os
from google.cloud import secretmanager_v1beta1 as secretmanager
import base64
import pymysql

client = error_reporting.Client()
secret_client = secretmanager.SecretManagerServiceClient()


def dict_lower(d):
    """
    This lowercases all keys in a dictionary, used because SF doesn't always have the same case for keys.

    :param d: A dictionary of Key-Value pairs
    :type d: dict
    :return: A dictionary of Key-Value pairs with all keys lowercased
    """
    return {k.lower(): v for k, v in d.items()}


def get_secret(project_id, secret_id, version_id):
    """
    :param project_id: The Google Cloud Project
    :type project_id: str
    :param secret_id: The Google Cloud Secret Id
    :type secret_id: str
    :param version_id: The version of the secret
    :type version_id: int
    :return: The Secret
    """
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
password = get_secret("project", "jc_salesforce_password", int(os.environ.get('password_version')))
environment = os.environ.get('environment')
token = get_secret("project", "jc_salesforce_token", int(os.environ.get('token_version')))
token_dev = get_secret("project", "jc_salesforce_token_dev", int(os.environ.get('token_dev_version')))
cloud_sql_connection_name = os.environ.get("cloud_sql_connection_name")
db_user = os.environ.get('project')
db_pass = get_secret("project", "website_sql_password", int(os.environ.get('sql_password_version')))
sql_ip = os.environ.get('sql_ip')
db_name = None

sf_live = Salesforce(username=email, password=password, security_token=token)
sf_dev = Salesforce(username=email_dev, password=password, security_token=token_dev, domain=environment)
sf = None


def find_lead(lead_email: str):
    """
    :param lead_email: Lead Email
    :type lead_email: str
    :return: Salesforce Lead Object
    :rtype: salesforce.Lead
    """
    # Get a lead using the registered email, if this fails, return none.
    try:
        lead_object = sf.Lead.get_by_custom_id("Email", lead_email)
        return lead_object
    except (exceptions.SalesforceResourceNotFound, ConnectionError):
        return None


# noinspection PyBroadException
def convert_lead(lead_id):
    """
    :param lead_id: Salesforce Lead Id
    :return: Status of lead conversion
    """
    # Convert an existing lead into a contact and account
    try:
        conversion_result = sf.apexecute('/Lead/{id}'.format(id='lead_id'), method='GET')
    except Exception:
        client.report("There was an error converting lead {lead_id} to a contact.".format(lead_id=lead_id))
        conversion_result = None
    return conversion_result


# noinspection PyDictCreation
def get_create_contact(contact: dict, account_id=None):
    """
    Check to see if a contact already exists with the given email. If it does, return
    else create a new contact

    :param contact: New user details
    :type contact: dict
    :param account_id: Salesforce account Id
    :type account_id: str
    :return: A Salesforce Contact
    :rtype: salesforce.Contact
    """

    try:
        existing_contact = sf.Contact.get_by_custom_id('Email', contact['email'])
        return existing_contact
    except (ConnectionError, exceptions.SalesforceResourceNotFound):
        sf_contact = {}
        sf_contact["LastName"] = contact['last_name']
        sf_contact['FirstName'] = contact['first_name']
        sf_contact['Email'] = contact['email']
        sf_contact['GACLIENTID__c'] = contact['client_id']
        sf_contact['GATRACKID__c'] = contact['track_id']
        sf_contact['GAUSERID__c'] = contact['id']
        created_contact: dict = sf.Contact.create(sf_contact)
        if created_contact['success']:
            return created_contact
        else:
            client.report('There was an error creating the contact. The response was {}'.format(json.dumps(
                created_contact)))
            return None


def get_create_account(business_name: str, contact: dict):
    """
    :param contact: The contact to assign to the account
    :type contact: dict
    :param business_name: The business name:
    :type business_name: str
    :return: Salesforce Account object
    :rtype: salesforce.Account
    """
    # noinspection SqlDialectInspection
    # Find all accounts that have the same name
    accounts = sf.query("SELECT Id FROM Account WHERE Name ='{name}'".format(name=business_name))
    # If there is only one account, return the account
    # If there are 0 or multiple accounts, create a new account
    if accounts['totalSize'] == 1:
        account = dict_lower(accounts['records'][0])
        account = sf.Account.get(account['id'])
    else:
        sf_account = {'Name': business_name}
        try:
            account = dict_lower(sf.Account.create(sf_account))
        except exceptions.SalesforceMalformedRequest as ex:
            client.report('There was a malformed request. Message: {} Error Code: {}'.format(ex.content[0]['message'],
                                                                                             ex.content[0]['errorCode'])
                          )
            return None
        if not account['success']:
            client.report('There was an error creating the account. The response was {response}'.format(
                response=json.dumps(account)))
            return None
        account = sf.Account.get(account['id'])
    return account


# noinspection PyDictCreation
def update_account(account: dict, data: dict, contact: dict):
    """
    :param contact: A Salsefoce Contact object
    :type contact: dict
    :param account: A Salesforce Account object
    :type account: Salesforce.Account
    :param data: The new contact data
    :type data: dict
    :return: The updated Salesforce account object
    :rtype: dict
    """
    sf_account = {}
    sf_account['Website'] = data['website']
    sf_account['Business_Employees__c'] = data['business_size']
    sf_account['Business_Type_POW__c'] = data['business_type']
    sf_account['npe01__One2OneContact__c'] = contact['id']
    sf.Account.update(account['id'], sf_account)
    return get_create_account(account['name'], data)


# noinspection PyDictCreation
def create_opportunity(account: dict, contact: dict, record_type: str):
    """
    :param record_type: The Opportunity Record Id
    :type record_type: str
    :param account: Salesforce Account
    :type account: dict
    :param contact: Salesforce Contact
    :type contact: dict
    :return: The new salesforce opportunity
    :rtype: dict
    """
    opportunity_value = get_opportunity_value(account['business_employees__c'])
    opportunity = {}
    opportunity['RecordTypeId'] = record_type
    opportunity['AccountId'] = account['id']
    opportunity['StageName'] = "Prospecting"
    opportunity['Name'] = "Pledge - " + account['name']
    opportunity['Amount'] = opportunity_value
    opportunity['CloseDate'] = str(date.today() + timedelta(days=28))
    opportunity['npsp__Primary_Contact__c'] = contact['id']
    return sf.Opportunity.create(opportunity)


def get_opportunity_value(employees):
    """
    :param employees: Number of employees from the form
    :return: The value of the opportunity
    :rtype: float
    """
    prices = {
	"100":100    
	}
    try:
        value = prices[employees]
    except KeyError:
        client.report('There was an error in the employee numbers. The number of employees was {value}'.format(
            value=employees))
        value = 50.00
    return value


def add_to_user_meta(database, user_id, key, value):
    """
    :param database: The database name
    :param user_id: The new User Id
    :param key: User metadata key
    :param value: User metadata value
    :return: None
    """
    connection = pymysql.connect(host=sql_ip,
                                 user=db_user,
                                 password=db_pass,
                                 db=database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor()
    sql = 'INSERT INTO ohine_usermeta (user_id, meta_key, meta_value) ' \
          'VALUES ("{uid}", "{key}", "{value}");'.format(uid=user_id, key=key, value=value)
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()


# noinspection PyShadowingNames
def run(event: dict, context: dict):
    """
    :param event: The data from the User form
    :type event: dict
    :param context: Event context
    :type context: dict
    """
    global sf
    data = eval(base64.b64decode(event['data']))
    data['url'] = data['url'].replace("\\/", "")
    data['website'] = data['website'].replace("\\", "")
    sf = sf_live if data['url'] in ["protectourwinters.uk", "pledge.protectourwinters.uk"] else sf_dev
    global db_name
    db_name = "site_live" if data['url'] in [
        "protectourwinters.uk", "pledge.protectourwinters.uk"] else "site_staging"
    lead = find_lead(lead_email=data['email'])
    if lead:
        if not lead['IsConverted']:
            ld_id = lead["Id"]
            convert_lead(ld_id)
        account = get_create_account(lead['Company'])
    else:
        contact = dict_lower(get_create_contact(data))
        account = get_create_account(data['business_name'], contact)
    if not account:
        return
    account = dict_lower(account)
    account = dict_lower(update_account(account, data, contact))
    record_id = "0124J000000hL3fQAE"
    opportunity = dict_lower(create_opportunity(account, contact, record_id))
    add_to_user_meta(db_name, data['id'], "sf_account_id", account['id'])
    add_to_user_meta(db_name, data['id'], "sf_opportunity_id", opportunity['id'])
