import logging
import sqlite3
import functools
import requests
import base64
import json
import os
import time
from datetime import datetime


def SetupLogging(starttime):
    log_filename = f"log_TMZconversie_{starttime.strftime('%Y-%m-%dT%H.%M.%S')}.csv"
    CreateTransactionDatabase
    if not os.path.isfile(log_filename):
        print(f'Setting up logging, creating new file: {log_filename}')
        with open(log_filename, 'w') as file:
            file.write('level;time;message;functionname;linenumber;TransactionID;Dossieritemnummer\n')
    else:
        print(f'Setting up logging, using previous log file: {log_filename}')
    
    # Create a custom logger
    logger = logging.getLogger('Conversie')
    logger.setLevel(logging.DEBUG)
    
    # Create handlers
    f_handler = logging.FileHandler(filename=log_filename, mode='a')
    f_handler.setLevel(logging.DEBUG)
    
    # Create formatters and add it to handlers
    f_format = logging.Formatter('{levelname};{asctime};{message};{funcName};{lineno};{TransactionID};{Dossieritemnummer}', style='{')
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(f_handler)
    return logger


class KnSubject_Fields:
    def __init__(self, row):
        self.StId = row['Typedossieritemnummer_FIN']
        self.Ds = row['Onderwerp_Samen']
        self.Da = datetime.strptime(row['Instuurdatum'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S")
        self.SbPa = row['Bestandsnaam']
        if row['Kenmerk1_FIN'] is not None:
            self.FvF1 = row['Kenmerk1_FIN']
        if row['Kenmerk2_FIN'] is not None:
            self.FvF2 = row['Kenmerk2_FIN']
        if row['Kenmerk3_FIN'] is not None:
            self.FvF3 = row['Kenmerk3_FIN']
    
class KnSubjectLink_Fields:
    def __init__(self, row):
        self.DoCRM = True
        if row['Medewerkernummer'] is not None:
            self.ToEM = True
            self.SfTp = 2
            self.SfId = row['Medewerkernummer']
        elif row['Werkgevernummer'] is not None:
            self.ToEr = True
            self.SfTp = 10
            self.SfId = row['Werkgevernummer']
        if row['Typedossieritemnummer_FIN'] in [92, 93, 101, 56, 3, 68]:
            self.AbId = row['VerzuimID_FIN']

class KnSubjectAttachment_Fields:
    def __init__(self, row, b64_filedata):
        self.FileName = row['Bestandsnaam']
        self.FileStream = b64_filedata


def TryRequest3Times(direction):
    # Probeer Request met Connector opnieuw als het niet in 1 keer lukt.
    def decorator_TR3T(func):
        @functools.wraps(func)
        def wrapper_TR3T(self, *args):
            number_of_attempts = 0
            response = None
            while number_of_attempts < 3:
                try:
                    number_of_attempts += 1
                    response = func(self, *args)
                except requests.exceptions.ReadTimeout:
                    print('\t\t\t\t\t\t\t\t\tReadTimeout', direction)
                    logger.warning(f'ReadTimeout {direction}', extra={'TransactionID': self.rows[self.index]['TransactionID'], 'Dossieritemnummer': self.rows[self.index]['Dossieritemnummer']})
                    continue
                except requests.exceptions.HTTPError:
                    print('\t\t\t\t\t\t\t\t\tHTTPError')
                    logger.warning(f'HTTPError {direction}', extra={'TransactionID': self.rows[self.index]['TransactionID'], 'Dossieritemnummer': self.rows[self.index]['Dossieritemnummer']})
                    continue
                break
            return response
        return wrapper_TR3T
    return decorator_TR3T



class NieuwDossier:
    def __init__(self, dossieritemnummer):
        self.dossieritemnummer = dossieritemnummer
        cursor.execute("""
                        SELECT * FROM Transactiontable
                        WHERE Dossieritemnummer IS ?
                        ORDER BY Bijlagetype ASC, BijlageID ASC
                        """, (self.dossieritemnummer,)
                       )
        self.rows = cursor.fetchall()
        print('Dossieritem', self.dossieritemnummer, 'TransactionID_0:',self.rows[0]['TransactionID'], 'Aantal bestanden:', len(self.rows))
        
        self.row_index = -1
        
        self.knsubject = {'KnSubject':{'Element':{'Fields':KnSubject_Fields(self.rows[0])}}}
        self.knsubject['KnSubject']['Element']['Objects'] = []
        self.knsubject['KnSubject']['Element']['Objects'].append({'KnSubjectLink':      {'Element':{'Fields':KnSubjectLink_Fields(self.rows[0])}}})
        self.knsubjectattachment = {'KnSubjectAttachment':{'Element':[]}}
        
        for index in range(len(self.rows)):
            self.index = index
            print('\t','TransactionID',self.rows[self.index]['TransactionID'], ', self.index:', self.index, 'Bestandsnaam:', self.rows[self.index]['Bestandsnaam'], 'BijlageID:', self.rows[self.index]['BijlageID'])
            logger.debug('Starting Transaction', extra={'TransactionID': self.rows[self.index]['TransactionID'], 'Dossieritemnummer': self.dossieritemnummer})
            response = self.GetSubjectFile()
            B64_Filedata = self.HandleResponse(response, 'Download')
            self.knsubjectattachment['KnSubjectAttachment']['Element'].append( {'Fields':KnSubjectAttachment_Fields(self.rows[self.index], B64_Filedata)} )
        self.knsubject['KnSubject']['Element']['Objects'].append(self.knsubjectattachment)
        self.json_data = json.dumps(self.knsubject, default=lambda obj: obj.__dict__, indent=2)
        #print(self.json_data)
        response = self.Upload_SubjectAttachment()
        self.Nieuw_dossieritemnummer = self.HandleResponse(response, 'Upload')
        if self.Nieuw_dossieritemnummer is not None:
            logger.info(f'Dossieritem created: {self.Nieuw_dossieritemnummer}', extra={'TransactionID': self.rows[0]['TransactionID'], 'Dossieritemnummer': self.dossieritemnummer})
            cursor.execute("UPDATE Transactiontable SET Nieuw_Dossieritem = ? WHERE Dossieritemnummer = ?", (self.Nieuw_dossieritemnummer, self.dossieritemnummer))
            connection.commit()
    
    @TryRequest3Times(direction='Download')
    def GetSubjectFile(self):
        if self.rows[self.index]['Bijlagetype'] == 'Reactie':
            url = f"https://12345.rest.afas.online/ProfitRestServices/subjectconnector/{self.rows[self.index]['Reactie']}/{self.rows[self.index]['Bijlagecode']}"
        elif self.rows[self.index]['Bijlagetype'] == 'Bijlage':
            url = f"https://12345.rest.afas.online/ProfitRestServices/subjectconnector/{self.rows[self.index]['Dossieritemnummer']}/{self.rows[self.index]['Bijlagecode']}"
        
        token = '<token><version>1</version><data>0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF</data></token>'  # O12345AA hrm live omgeving, gebruiker 12345.9168 (Robby Robot), rechten Robot
        headers = {'Authorization': 'AfasToken ' + str(base64.b64encode(token.encode("utf-8")), "utf-8")}
        
        return requests.get(url=url, headers=headers, timeout=60)
    
    @TryRequest3Times(direction='Upload')
    def Upload_SubjectAttachment(self):
        url = 'https://12345.rest.afas.online/ProfitRestServices/connectors/KnSubject'
        token = '<token><version>1</version><data>0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF</data></token>' # O12345AC financieel omgeving, gebruiker 12345.hl, rol Robot
        headers = {'Authorization': 'AfasToken '+str(base64.b64encode(token.encode("utf-8")), "utf-8") }
        return requests.post(url=url, headers=headers, data=self.json_data, timeout=60)
        
    
        
    def HandleResponse(self, Response, direction):
        def error_to_SQL_and_Log(direction, status_code, errorNumber, externalMessage, profitLogReference, TransactionID, Dossieritemnummer):
            logger.error(f"Unsuccesful {direction}. {status_code} {externalMessage}", extra={'TransactionID': TransactionID, 'Dossieritemnummer': Dossieritemnummer})
            cursor.execute("""
                UPDATE Transactiontable
                SET
                status_code = ?,
                errorNumber = ?,
                externalMessage = ?,
                profitLogReference = ?
                WHERE TransactionID = ?
                """,
                (f'{direction} {status_code}', errorNumber, externalMessage, profitLogReference, TransactionID)
                           )
            connection.commit()
        
        response_code_direction = {'Download':requests.codes.ok, 'Upload':requests.codes.created}
        response_jsonitems_direction = {
            'Download':['filedata'],
            'Upload':['results','KnSubject','SbId']
            }
        if isinstance(Response, requests.models.Response):
            if Response.status_code == response_code_direction[direction]:
                response_dict = Response.json()
                for item in response_jsonitems_direction[direction]:
                    response_dict = response_dict[item]
                return response_dict
            else:
                if Response.text:
                    response_message = json.loads(Response.text)
                else:
                    errorNumber_nomessage_direction = {'Download':12, 'Upload':22}
                    response_message = {'errorNumber':errorNumber_nomessage_direction[direction],'externalMessage':'No response message :(', 'profitLogReference':''}
                error_to_SQL_and_Log(direction, Response.status_code,  response_message['errorNumber'],  response_message['externalMessage'],  response_message['profitLogReference'],  self.rows[self.row_index]['TransactionID'], self.rows[self.row_index]['Dossieritemnummer'])
                return None
        else:
            print('\t\t\t\t\t\t\t\tNIET een instance')
            errorNumber_noresponse_direction = {'Download':11, 'Upload':21}
            error_to_SQL_and_Log(direction, '', errorNumber_noresponse_direction[direction], 'Unsuccesful {direction}', '', self.rows[self.index]['TransactionID'], self.rows[self.index]['Dossieritemnummer'])
            return None
        





if __name__ == '__main__':
    # Hier begint het script daadwerkelijk
    
    # Bepaald starttijd, gebruik in logger en copy databasebestand.
    starttime = datetime.now()
    print('Runtime begonnen om:',starttime)
    logger = SetupLogging(starttime)
    os.popen(f"copy ConversieData.db ConversieData_Backup_{starttime.strftime('%Y-%m-%dT%H.%M.%S')}.db")
    time.sleep(1)
    
    # Verbind met database en haal lijst van (unieke) dossieritemnummers op
    #connection = sqlite3.connect('ConversieData.db')
    connection = sqlite3.connect("ConversieData.db")
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    cursor.execute("""
        SELECT * FROM Transactiontable
        WHERE
        Nieuw_Dossieritem IS NULL
        AND VerzuimID_FIN IS NULL
        AND Workflow IS NOT NULL
        """)    
    dossieritems = [dos['Dossieritemnummer'] for dos in cursor.fetchall()]
    dossieritems = list(dict.fromkeys(dossieritems)) # Unique values (like Set()), Order preserving
    print('Totaal aantal unieke dossieritems:', len(dossieritems))
    
    # Begin met loopen door de lijst met dossieritemnummers
    loop_aantal = 1000000
    loopstartindex = 0
    loopstarttime = time.time()
    print('Start loop:')
    for index, dossieritem in enumerate(dossieritems):
        if loopstartindex <= index and index < loopstartindex + loop_aantal:
            # Een try-except clausule rondom het daadwerkelijk ophalen en aanmaken van dossieritem, waarbij een error wordt opgevangen en in het log en database-bestand wordt weggeschreven.
            try:
                # Hier wordt daadwerkelijk de Class aangeroepen waarin de bestanden worden gedownload en het nieuwe dossieritem wordt ingeschoten.
                logger.debug('Starting Transaction', extra={'TransactionID': '', 'Dossieritemnummer': dossieritem})
                nieuw = NieuwDossier(dossieritem)
                if hasattr(nieuw, 'Nieuw_dossieritemnummer'):
                    print('Nieuw_dossieritemnummer',nieuw.Nieuw_dossieritemnummer)
            except Exception as exception:
                logger.critical('Er ging iets mis::'+str(type(exception))+':'+str(exception.args), extra={'TransactionID': '', 'Dossieritemnummer': dossieritem})
                cursor.execute("UPDATE Transactiontable SET externalMessage = ? WHERE Dossieritemnummer = ?", (str(exception.args), dossieritem))
                connection.commit()
            
    
    # Afsluiting van script.
    elapsed_time = time.time() - loopstarttime
    print('elapsed_time',elapsed_time)
    print('elapsed_time per item',elapsed_time/loop_aantal)
    
    cursor.close()
    connection.close()
    logging.shutdown()
