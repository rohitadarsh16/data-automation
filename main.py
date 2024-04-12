import requests, json, os

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import logging

# Configure the logging module
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


import datetime

uri = "mongodb+srv://deepak:ZeXtafvF7jfYFabr@cluster0.twcbhex.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'), tls=True, tlsAllowInvalidCertificates=True)
db = client.rezonanz


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


def sendmail(recipient_email, update, collectionName, erorUpdate):

    sender_email = 'deepakpythonwork@gmail.com'
    sender_password = 'uzed ghqu tkxz mjxf '
    email_template_path = "index.html"
    with open(email_template_path, "r") as template_file:
        email_template = template_file.read()
    email_template = email_template.replace("{row number}", str(update))
    email_template = email_template.replace("{notebook}", str(collectionName))
    email_template = email_template.replace("{bug}", str(erorUpdate))
    subject = 'Update ' + ' ' + str(erorUpdate)  


    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = subject

    message.attach(MIMEText(email_template, 'html'))
    # with open(attachment_path, "rb") as attachment_file:
    #     attachment = MIMEApplication(attachment_file.read(), _subtype="xlsx")
    #     attachment.add_header('Content-Disposition', 'attachment', filename=attachment_path)
    #     message.attach(attachment)

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()  
        server.login(sender_email, sender_password)  
        server.sendmail(sender_email, recipient_email, message.as_string()) 


def getdata(start_date, end_date, rows, page, customerID):

    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-GB,en;q=0.5',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
    'Referer': 'https://vds.issgovernance.com/vds/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    }

    
    params = {
    'customerID': customerID,
    'fromDate': start_date,
    'toDate': end_date,
    'signMeeting': 'All',
    'liveSiteYN': '1',
    'random': '0.7307961892016783',
    'locale': 'en',
    'MeetingTypeList': '',
    'CountryList': '',
    'VotedList': '',
    'actionCode': '101',
    'sessionToken': '1706393802765',
    '_search': 'false',
    'nd': '1706393867935',
    'rows': rows,
    'page': page,
    'SortByColumn': 'CompanyName',
    'OrderBy': 'asc',
}

    response = requests.get('https://vds.issgovernance.com/vds/api/getVdsData/14', params=params,headers=headers)
    return response.json()['data']



def getCompanydata(MultipleFundIDs, MeetingId, customerID):    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-GB,en;q=0.5',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
        'Referer': 'https://vds.issgovernance.com/vds/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',    
    }

    
    params = {
        'customerID': customerID,
        'fromDate': '',
        'toDate': '',
        'fundValue': MultipleFundIDs,
        'liveSiteYN': '1',
        'signMeeting': 'All',
        'signVote': 'All',
        'meetingID': MeetingId,
        'random': '0.9265552175773822',
        'locale': 'en',
        'actionCode': '114',
        'sessionToken': '1706393802765',
        '_search': 'false',
        'nd': '1706394317474',
        'rows': '2000',
        'page': '1',
        'SortByColumn': 'BallotItemNumber',
        'OrderBy': 'asc',
    }
    
   

    response = requests.get('https://vds.issgovernance.com/vds/api/getVdsData/7', params=params, headers=headers)

    return response.json()['data']
def getTotalrow(start_date, end_date, rows, page, customerID):
     TRow= getdata(start_date, end_date, rows, page, customerID)[0]['TotalRows']
     return TRow



def ExtractData(collectionName , customerID, start_date, end_date,rows):
    start_row = 0
    # rows = getTotalrow(start_date, end_date, 20, 1)
    data = getdata(start_date, end_date, rows, 1, customerID)
    print(f"Total rows {rows}")
    logging.info(f"Total rows {rows}")  
    for i in range(start_row, len(data)):
        dataDict = {"Company": data[i]['CompanyName'], "Ticker": data[i]['Ticker'], "Country": data[i]['Country'], "SecurityID": data[i]['SecurityID'], "MeetingDate": data[i]['MeetingDate'], "meetingType": data[i]['MeetingType'], "FundName": data[i]['FundName'], "MultipleFundIDs": data[i]['MultipleFundIDs'], "MeetingID": data[i]['MeetingID'] }
        if "," in dataDict['MultipleFundIDs']:
            dataDict['MultipleFundIDs'] = dataDict['MultipleFundIDs'].replace(',' , '||')
        companyData = getCompanydata(dataDict['MultipleFundIDs'], dataDict['MeetingID'], customerID)
        tempdata = []
        FundName = ''
        inudstryType = ''
        RecordDateDetail = ''
        for j in companyData:
            if '<' in j['Notes'] or 'br' in j['Notes']:
                j['Notes'] = j['Notes'].replace('<br>', ' ')
                j['Notes'] = j['Notes'].replace('NA', ' ')
            FundName = j['FundNames']
            inudstryType = j['SixDigitSectorType']
            RecordDate =  j['RecordDateDetail']
            filtterdata = {    
            "FundFootnoteSymbol":j['FundFootnoteSymbol'],
            "MeetingTypeDetail": j['MeetingTypeDetail'],
            "CompanyID": j['CompanyID'],
            "ShareholderProposal": j['ShareholderProposal'],
            "FundFootnoteText": j['FundFootnoteText'],
            "SeqNumber": j['SeqNumber'],
            "MeetingFootnoteSymbol": j['MeetingFootnoteSymbol'],
            "securityID": j['SecurityIDDetail'],
            "EsgPillar": j['EsgPillar'],
            "MgmtRec": j['MgtRecVote'],
            "Notes":j['Notes'],
            "ProposalSubCategory": j['ProposalSubCategory'],
            "ProposalFootnoteText": j['ProposalFootnoteText'],
            "CountryDetail": j['CountryDetail'],
            "SignificantProposalYN": j['SignificantProposalYN'],
            "Vote": j['ClientVoteList'],
            "MeetingFootnoteText": j['MeetingFootnoteText'],
            "ProposalCategory": j['ProposalCategory'],
            "SharesVotedList": j['SharesVotedList'],
            "Item#": j['BallotItemNumber'],
            "VoteResult": j['VoteResult'],
            # "MeetingDateDetail": j['MeetingDateDetail'],
            "ItemOnAgendaID": j['ItemOnAgendaID'],
            "ProposalFootnoteSymbol": j['ProposalFootnoteSymbol'],
            "Proposal": j['Proposal'],
            "ResearchNotes": j['ResearchNotes'],
            "ContextualNote": j['ContextualNote'], 
        }
        tempdata.append(filtterdata)
        dataDict['FundName']= FundName
        dataDict['RecordDate'] = RecordDate
        dataDict['proposaldata'] = tempdata
        dataDict['IndustrySector']= inudstryType
        dataDict['created_at'] = datetime.datetime.now()
        db[collectionName].insert_one(dataDict)
        print(f"processed  {i + 1} of {rows} rows" )
        logging.info(f"processed  {i + 1} of {rows} rows" )
   

start_date = '2023-01-01'
end_date = '2024-03-01'


dashboarddata = [
    {"customerID": "Mjg1OA==/", "collectionName": "Amundi_test"},
    {"customerID": "MjIwNw==", "collectionName": "HSBC_test"},
    {"customerID": "MjU2NQ==/", "collectionName": "lgim_test"},
    {"customerID": "MTA4MjY3MDQ=", "collectionName": "Federated_Hermes_test"},
    {"customerID": "MTcyOQ==/", "collectionName": "Praxis_Mutual_Funds_test"},
    {"customerID": "MTY0MQ==/", "collectionName": "Calvert_test"},
    {"customerID": "Mzk3MA==/", "collectionName": "Invesco_test"},
    {"customerID": "MzM3MQ==", "collectionName": "Trillium_Asset_Management_test"},
    {"customerID": "NDU4NQ==/", "collectionName": "DWS_test"},
]

def sleep_until_next_day():
    # Get current date and time
    now = datetime.datetime.now()

    # Calculate the time until the next day
    next_day = now + timedelta(days=1)
    next_day_start = datetime(next_day.year, next_day.month, next_day.day, 0, 0, 0)
    time_until_next_day = (next_day_start - now).total_seconds()

    # Sleep until the next day
    print(f"Sleeping until {next_day_start}")
    logging.info(f"Sleeping until {next_day_start}")
    time.sleep(time_until_next_day)



index = 0
while True:
    if index == 0:
        for data in dashboarddata:
            
            rows = getTotalrow(start_date, end_date, 20, 1, data['customerID'])
            print(f"Total rows {rows} , {start_date}, {end_date} {data['customerID']} {data['collectionName']}")
            logging.info(f"Total rows {rows} , {start_date}, {end_date} {data['customerID']} {data['collectionName']}")
            ExtractData(data['collectionName'], data['customerID'], start_date, end_date, rows)
            
        index += 1
    else:
        start_date = str(end_date)
        end_date = datetime.datetime.now().strftime('%Y-%m-%d') 
        print('date next date', end_date, start_date)
        logging.info(f"date next date {end_date} {start_date}")
        for data in dashboarddata:
            try:
                rows = getTotalrow(start_date, end_date, 20, 1, data['customerID'])
                print(f"Total rows {rows} , {start_date}, {end_date} {data['customerID']} {data['collectionName']}")
                logging.info(f"Total rows {rows} , {start_date}, {end_date} {data['customerID']} {data['collectionName']}")
            except Exception as e:
                sendmail('rohit45deepak@gmail.com', rows, data['collectionName'], 'no data found')
                logging.error(f"no data found")
                continue
            try:
                ExtractData(data['collectionName'], data['customerID'], start_date, end_date, rows)
            except Exception as e:
                sendmail('rohit45deepak@gmail.com', rows, data['collectionName'], e)
                logging.error(f"Error: {e}")

    sleep_until_next_day()
    
    