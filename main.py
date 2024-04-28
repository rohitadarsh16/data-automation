import requests, json, os

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import logging
logging.basicConfig(filename='dataprocess.log', level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')



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



from datetime import datetime

def convert_string_to_datetime(date_string):
    if date_string:
        return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S.%f")
    else:
        return None
    
   
def ExtractData(collectionName, customerID,uid ,start_date, end_date, rows):
    start_row = 0
    datalist = []
    data = getdata(start_date, end_date, rows, 1, customerID)
    print(f"Total rows {rows}")
    logging.info(f"Total rows {rows}")
    for i in range(start_row, len(data)):
        dDict = {"Company": data[i]['CompanyName'], "Ticker": data[i]['Ticker'], "Country": data[i]['Country'], "SecurityID": data[i]['SecurityID'], "MeetingDate": data[i]['MeetingDate'], "meetingType": data[i]['MeetingType'], "MultipleFundIDs": data[i]['MultipleFundIDs'], "MeetingID": data[i]['MeetingID'] }
        
        
        
        fundIDs = [] 
        voteleveldata = []
        if "," in dDict['MultipleFundIDs']:
            fundIDs = dDict['MultipleFundIDs'].split(',')
        else:
            fundIDs.append(dDict['MultipleFundIDs'])

        for fundID in fundIDs:
            dataDict = {"Company": data[i]['CompanyName'], "Ticker": data[i]['Ticker'], "Country": data[i]['Country'], "SecurityID": data[i]['SecurityID'], "MeetingDate": data[i]['MeetingDate'], "meetingType": data[i]['MeetingType'], "MultipleFundIDs": data[i]['MultipleFundIDs'], "MeetingID": data[i]['MeetingID']}
            AllData ={}
            companyData = getCompanydata(fundID, dataDict['MeetingID'], customerID)
            
            tempdata = []
            companyleveData = []
            meetinglevelData = []
            proposalLevelData = []
            voteList = []
            FundFootnoteS = []
            FundFootnoteT = []
            Notes= []
            ResearchNotes = []
            ContextualNote = []
            voteLevelFundData = {}
            FundName = ''
            inudstryType = ''
            RecordDateDetail = ''
             
            
            for j in companyData:
                if '<' in j['Notes'] or 'br' in j['Notes']:
                    j['Notes'] = j['Notes'].replace('<br>', ' ')
                    j['Notes'] = j['Notes'].replace('NA', ' ')
                companyleveData = [{'CompanyID':j['CompanyID'], 'CompanyName':j['CompanyNameDetail'], 'TickerDetail':j['TickerDetail'], 'CountryDetail':j['CountryDetail']}]
                
                meetinglevelData = [{'MeetingTypeDetail': j['MeetingTypeDetail'], 'MeetingDate': convert_string_to_datetime(j['MeetingDateDetail']),  "MeetingFootnoteText": j['MeetingFootnoteText'], "RecordDateDetail":convert_string_to_datetime( j['RecordDateDetail'])}]
               
                proposalLevel = {"ShareholderProposal": j['ShareholderProposal'],"EsgPillar": j['EsgPillar'],"MgtRecVote": j['MgtRecVote'] ,"Proposal": j['Proposal'], "ProposalSubCategory": j['ProposalSubCategory'], "ProposalFootnoteText": j['ProposalFootnoteText'], "ProposalFootnoteSymbol": j['ProposalFootnoteSymbol'], "SignificantProposalYN": j['SignificantProposalYN'],"ProposalCategory": j['ProposalCategory'], "BallotItemNumber": j['BallotItemNumber'], "SeqNumber":j["SeqNumber"]}
                proposalLevelData.append(proposalLevel)
                # votelevel = {"SecurityIDDetail": j["SecurityIDDetail"], "FundId" : fundID, "FundNames": j["FundNames"], "SharesVotedList": int(j['SharesVotedList']),"ClientVoteList" : j["ClientVoteList"] , "FundFootnoteSymbol":j['FundFootnoteSymbol'], "FundFootnoteText": j['FundFootnoteText'] ,  "Notes":j['Notes'], "ResearchNotes": j['ResearchNotes'], "ContextualNote": j['ContextualNote']}
                voteLevelFundData["SecurityIDDetail"] = j["SecurityIDDetail"]
                voteLevelFundData["FundId"] = fundID
                voteLevelFundData["FundNames"] = j["FundNames"]
                voteLevelFundData["SharesVotedList"] = j['SharesVotedList']

                voteList.append(j["ClientVoteList"])
                FundFootnoteS.append(j['FundFootnoteSymbol'])
                FundFootnoteT.append( j['FundFootnoteText'])
                Notes.append( j['Notes'])
                ResearchNotes.append(j['ResearchNotes'])
                ContextualNote.append(j['ContextualNote'])
                # voteLevelFundData.append(votelevel)
            voteLevelFundData['voteList'] = voteList
            voteLevelFundData['FundFootnoteS'] = FundFootnoteS
            voteLevelFundData['FundFootnoteT'] = FundFootnoteT
            voteLevelFundData['Notes'] = Notes
            voteLevelFundData['ResearchNotes'] = ResearchNotes
            voteleveldata.append(voteLevelFundData)

            AllData['CompanyLevel'] = companyleveData
            AllData['MeetingLevel'] = meetinglevelData
            AllData['ProposalLevel'] = proposalLevelData
        AllData['uid'] = uid
        AllData['VoteLevelFundData'] = voteleveldata
        AllData['created_at'] = datetime.now()

        db[collectionName].insert_one(AllData)
          
        print(f"processed  {i + 1} of {rows} rows" )
        logging.info(f"processed  {i + 1} of {rows} rows" )


start_date = '2024-01-01'
end_date = '2024-04-27'


dashboarddata = [
    {"customerID": "NDI0NQ==/", "collectionName": "swisscanto_test", 'investor_uuid': '53b90b50-8c66-11ee-b12d-b3d0f7682273'} ,
    {"customerID": "MTE0MjA=/", "collectionName": "creditsuisse_test", 'inverstor_uuid': '219fdeb0-d021-11ee-a1e4-2441dc8a2002'},
    {"customerID": "MTAxODE=/", "collectionName": "swislife_test", 'inverstor_uuid': '219fa026-d021-11ee-a1e4-2441dc8a2002'},
    # {"customerID": "MTEyODk=", "collectionName": "Ostrum_test"},
    # {"customerID": "MjI1Ng==", "collectionName": "Pyrford_test"},
    # {"customerID": "MTcy", "collectionName": "Genesis_test"},
    # {"customerID": "ODg3MDA=", "collectionName": "BT_AUS_test"},
    # {"customerID": "ODI4OQ==", "collectionName": "Carmignac_Voting_Disclosure_test"},
    # {"customerID": "OTQ0Ng==", "collectionName": "gqg_partners_test"},
    # {"customerID": "ODkyNA==", "collectionName": "Mi_test"},
    # {"customerID": "ODA1MA==", "collectionName": "Mirabaud_test"},
    # {"customerID": "OTAyNg==", "collectionName": "mirova_test"},
    # {"customerID": "MjMyMA==", "collectionName": "comgest_test"},
    # {"customerID": "MzY2MDA=", "collectionName": "Paedagogernes_Pension_test"},
    # {"customerID": "ODg3OQ==", "collectionName": "Sycomore_test"},
    # {"customerID": "MTUyMA==", "collectionName": "Pzena Investment Management"},
    # {"customerID": "NzcyMA==", "collectionName": "Kempen"},
    # {"customerID": "ODI2NzA=", "collectionName": "PNO"},
    # {"customerID": "NzYxNA", "collectionName": "Unigestion"},
    # {"customerID": "NTg0", "collectionName": "Marathon Asset Management"},
    # {"customerID": "MzM3MQ==", "collectionName": "Trillium Asset Management"},
    # {"customerID": "MTMxMzk=", "collectionName": "T. Rowe Price Investment Management, Inc. (TRPIM)"},
    # {"customerID": "ODI2NzAy", "collectionName": "Industriens Pension"},
    # {"customerID": "MTI3NzI=", "collectionName": "Santander Asset Management"}
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
            ExtractData(data['collectionName'], data['customerID'], data['investor_uuid'],start_date, end_date, rows)
            
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
    
    