import requests
from base64 import b64encode
import re
import zeep
import json
from datetime import datetime
from unicodedata import normalize
from os import path, remove
import pandas as pd
from time import sleep


# Definieer hier een paar constanten:
url_Base = "https://12345.rest.afas.online/ProfitRestServices/connectors/"
token = "<token><version>1</version><data>0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDE</data></token>"
Request_headers = {'Authorization': 'AfasToken '+str(b64encode(token.encode("utf-8")), "utf-8") }

timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
LogErrorFile = path.join('Logs','Log_Error_BIGcontrole_{0}.csv'.format(timestamp))
LogTransactionsFile = path.join('Logs','Log_Transactions_BIGcontrole_{0}.csv'.format(timestamp))




# Hieronder worden een aantal functies en classes gedefinieerd. Dit zijn hulp-functies voor het proces. Helemaal benenden onder if __name__ == '__main__': begint het proces daadwerkelijk.


def LogError(dossieritemnummer, tekst):
    # Sla relevante info op in csv-bestand, zoals wanneer een persoon wordt afgekeurd (voor interne controle of alles dan goed gaat) of wanneer een connector niet goed werkt.
    
    if not path.exists(LogErrorFile):
        with open(LogErrorFile ,'w', encoding="utf-8") as newfile:
            newfile.write('Dossieritemnummer;Bericht' + "\n")
        
    with open(LogErrorFile ,'a', encoding="utf-8") as file:
        file.write(str(dossieritemnummer) + ';' + str(tekst) + "\n")

def RemoveLogIfIrrelevant():
    # Verwijder Logbestand als er geen regels in staan (op de standaard kolomtitels en Aantal Taken-regel na).
    
    with open(LogErrorFile ,'r') as file:
        inhoud = file.readlines()
    if len(inhoud) == 2:
        remove(LogErrorFile)



def CreateTransactionsCSV(Tasks):
    # Maak Pandas dataframe zodat relevante gegevens opgeslagen worden en de voorgang in bijgehouden kan worden.
        
    DossierItemNr = []
    BIGnummer = []
    Medewerker = []
    for Task in Tasks:
        DossierItemNr.append(Task['DossierItemNr'])
        Medewerker.append(Task['Medewerker'])
        BIGnum, _ = Onderwerp_Uit_Elkaar_Trekken(Task['Onderwerp'])
        BIGnummer.append(BIGnum)
    
    df = pd.DataFrame ({
        'DossierItemNr':  DossierItemNr,
        'Medewerker': Medewerker,
        'BIGnummer': BIGnummer,
        })
    df['Goedkeuren'] = None
    df['Reden'] = None
    df['HeeftMaatregel'] = None
    df['DoneAt'] = None
    df.to_csv(LogTransactionsFile, index=False, sep=';')
    return df



def TryRequest3Times(Request):
    # Probeer Request met Connector of Webservice opnieuw als het niet in 1 keer lukt. 
    number_of_attempts = 0
    while number_of_attempts < 3:
        try:
            number_of_attempts += 1
            response = Request
        except requests.exceptions.ReadTimeout:
            print('\t\t\t\t\t\t\t\t\tReadTimeout')
            LogError('-','Readtimeout (Profit connector)')
            continue
        except TimeoutError:
            print('\t\t\t\t\t\t\t\t\tTimeoutError')
            LogError('-','TimeoutError (Webservice BIG-register)')
            continue
        break
    if isinstance(response, requests.models.Response):
        PrintResponseUnsuccesful(response)
    return response


def PrintResponseUnsuccesful(response):
    if response.status_code == requests.codes.ok or response.status_code == requests.codes.created:
        pass
        #print("Request succesvol uitgevoerd.")
    else:
        LogError('Request','post response = ' + str(response.status_code) + response.reason)
        for key, value in response.json().items():
            print(str(key) + ' = ' + str(value))

    
def GetAllTasks() -> list:
    AllTasks = []
    
    Request_Number = 0
    Number_of_Items = 100
    # Zolang Number_of_Items gelijk is aan 100 zijn er (waarschijnlijk) nog meer dossieritems op te halen. Daarom wordt het ophalen herhaald totdat er minder dossieritems opgehaald worden. 
    while Number_of_Items == 100:
        
        # Probeer Request met Connector opnieuw als het niet in 1 keer lukt. 
        number_of_attempts = 0
        while number_of_attempts < 3:
            try:
                number_of_attempts += 1
                response = requests.get(url=url_Base+"MijnTaken?skip={0}&take=100".format(100*Request_Number), headers=Request_headers, timeout=6)
            except requests.exceptions.ReadTimeout:
                print('\t\t\t\t\t\t\t\t\tReadTimeout')
                LogError('-','Readtimeout (Profit connector) GetAllTasks')
                continue
            break
        
        list_Dossieritems = response.json()["rows"]
        AllTasks.extend(list_Dossieritems)
        Number_of_Items = len(list_Dossieritems)
        Request_Number += 1
    LogError('-','Aantal Taken = ' + str(len(AllTasks)))
    return AllTasks


def Onderwerp_Uit_Elkaar_Trekken(onderwerp):
    list_Onderwerp = re.split(r';',onderwerp)
    list_Onderwerp = list_Onderwerp[1:-1]
    
    dict_Namen = {
        'Achternaam' : list_Onderwerp[0],
        'Geboortenaam' : list_Onderwerp[1],
        'BIGAchternaam' : list_Onderwerp[2],
        }
    #Geslacht = list_Onderwerp[3]
    #Geboortedatum = list_Onderwerp[4]
    #Beroep = list_Onderwerp[5]
    BIGnummer = list_Onderwerp[6]
    return BIGnummer, dict_Namen



class BIGcontrole:
    def __init__(self, in_BIGnummer, in_dict_Namen):
        # Hier staat de logica achter het goed- of afkeuren van een zorgverlener.
        # De zorgverlener doorloopt het onderstaande proces en kan om verschillende redenen afgekeurd worden.
        # Pas als het einde van het proces is behaald zonder afgekeurd te worden wordt de zorgverlener goedgekeurd.
                
        self.in_BIGnummer = in_BIGnummer
        self.in_dict_Namen = in_dict_Namen #Dit is nodig voor de IsNameValid()-check
        
        self.RegisterResponse = self.BIGregister_MakeRequest()
        if self.RegisterResponse == None:
            self.Goedkeuren = False
            self.Reden = 'Met het registratienummer is geen zorgverlener in het BIG-register gevonden.'
            LogError(Dossieritemnummer, self.Reden)
        else:
            self.BIGRegisterInfo = self.BIGregister_ProcesResponse()
            if not self.IsNameValid(self.in_dict_Namen, self.BIGRegisterInfo['BIGachternaam']):
                self.Goedkeuren = False
                self.Reden = 'De naam in Profit en de naam in het BIG-register komen niet overeen.'
                LogError(Dossieritemnummer, self.Reden)
            else:
                if self.BIGRegisterInfo['MaatregelTekst'] != None:
                    self.Goedkeuren = False
                    self.Reden = 'De zorgverlener is een maatregel opgelegd.'
                    LogError(Dossieritemnummer, self.Reden)
                    self.HeeftMaatregel = True
                else:
                    self.Goedkeuren = True
                    self.Reden = ''
                    self.HeeftMaatregel = False
                    
    
    def BIGregister_MakeRequest(self):
        client = zeep.Client(wsdl='https://webservices.cibg.nl/Ribiz/OpenbaarV4.asmx')
        request_data = {
            'WebSite': 'Ribiz',
            'RegistrationNumber': self.in_BIGnummer,
            }
        
        # Probeer Request met Webservice opnieuw als het niet in 1 keer lukt. 
        number_of_attempts = 0
        while number_of_attempts < 3:
            try:
                number_of_attempts += 1
                response = client.service.ListHcpApprox4(**request_data)
            except TimeoutError:
                print('\t\t\t\t\t\t\t\t\tTimeoutError')
                LogError('-','TimeoutError (Webservice BIG-register)')
                continue
            break

        
        #print(response)
        if response:
            if len(response) == 1:
                return response[0]
            else:
                LogError(Dossieritemnummer, '\t\t\tMeerdere personen gevonden met '+self.in_BIGnummer)
                return None
        else:
            LogError(Dossieritemnummer, '\t\t\tGeen persoon gevonden met '+self.in_BIGnummer)
            return None
    
    def BIGregister_ProcesResponse(self):
        Beroepsgroepcode = {'01':'Arts','02':'Tandarts','03':'Verloskundig','04':'Fysiotherapeut','16':'Psychotherapeut','17':'Apotheker','18':'Apotheekhoudend arts','83':'Apothekersassistent','25':'Gz-psycholoog','30':'Verpleegkundige','81':'Physician assistant','85':'Tandprothetica','86':'Verzorgenden individuele gezondheidszorg','87':'Optometrist','88':'Huidtherapeut','89':'Diëtist','90':'Ergotherapeut','91':'Logopedist','92':'Mondhygiënist','93':'Oefentherapeut Mensendieck','94':'Oefentherapeut Cesar','95':'Orthoptist','96':'Podotherapeut','97':'Radiodiagnostisch laborant','98':'Radiotherapeutisch laborant'}
        Specialisme_code = {'2':'allergologie','3':'anesthesiologie','4':'algemene gezondheidszorg','5':'medische milieukunde','6':'tuberculosebestrijding','7':'arbeid en gezondheid','8':'arbeid en gezondheid - bedrijfsgeneeskunde','10':'cardiologie','11':'cardio-thoracale chirurgie','12':'dermatologie en venerologie','13':'leer van maag-darm-leverziekten','14':'heelkunde','15':'huisartsgeneeskunde','16':'inwendige geneeskunde','17':'jeugdgezondheidszorg','18':'keel- neus- oorheelkunde','19':'kindergeneeskunde','20':'klinische chemie','21':'klinische genetica','22':'klinische geriatrie','23':'longziekten en tuberculose','24':'medische microbiologie','25':'neurochirurgie','26':'neurologie','30':'nucleaire geneeskunde','31':'oogheelkunde','32':'orthopedie','33':'pathologie','34':'plastische chirurgie','35':'psychiatrie','39':'radiologie','40':'radiotherapie','41':'reumatologie','42':'revalidatiegeneeskunde','43':'maatschappij en gezondheid','44':'sportgeneeskunde','45':'urologie','46':'obstetrie en gynaecologie','47':'verpleeghuisgeneeskunde','48':'arbeid en gezondheid - verzekeringsgeneeskunde','50':'zenuw- en zielsziekten','53':'dento-maxillaire orthopaedie','54':'mondziekten en kaakchirurgie','55':'maatschappij en gezondheid','56':'medische zorg voor verstandelijk gehandicapten','60':'ziekenhuisfarmacie','61':'klinische psychologie','62':'interne geneeskunde-allergologie','66':'Acute zorg bij somatische aandoeningen', '67':'Intensieve zorg bij somatische aandoeningen'}
        
        BIGachternaam = self.RegisterResponse['BirthSurname']
        Geslacht = self.RegisterResponse['Gender']
        
        # Hier kun je een melding maken in het Errorlog als er meerdere bignummers horen bij een persoon. Periodiek kan het handig zijn om dit aan te zetten maar meestal is het niet nodig.
        if len(self.RegisterResponse['ArticleRegistration']['ArticleRegistrationExtApp']) > 1:
            LogError(Dossieritemnummer, 'Deze persoon heeft ' + str(len(self.RegisterResponse['ArticleRegistration']['ArticleRegistrationExtApp'])) + ' bignummers. Controleer of de juiste gebruikt wordt.')

        for Article in self.RegisterResponse['ArticleRegistration']['ArticleRegistrationExtApp']:
            
            # pak de registratie (Article) die overeenkomt met het bignummer uit Profit. (Origineel)
            #if str(Article['ArticleRegistrationNumber']) in self.in_BIGnummer:
            
            # pak de registratie die gelijk is aan tandarts.
            if Article['ProfessionalGroupCode'] == '02':
                # Artikel bijbehorend aan het goede bignummer gevonden. Voor dit artikel moeten de gegevens worden opgeslagen
                # Gegevens uit ArticleRegistrationExtApp halen
                BIGnummerOut = str(Article['ArticleRegistrationNumber'])
                Ingangsdatum = Article['ArticleRegistrationStartDate'].strftime('%Y-%m-%d')
                if Article['ArticleRegistrationEndDate'] != datetime(1, 1, 1, 0, 0) and Article['ArticleRegistrationEndDate'] < datetime.now():
                    BIGGeldig = False
                    Einddatum = Article['ArticleRegistrationEndDate'].strftime('%Y-%m-%d')
                else:
                    BIGGeldig = True
                    Einddatum = None
                Beroepsgroep = Beroepsgroepcode[Article['ProfessionalGroupCode']]
        
        
        # Kan volgens mij weg omdat er al gezocht wordt op dit bignummer dus kan het niet afwijken
        # Controle of Bignummer uit Profit gelijk is aan bignummer in Register
        #if str(Article['ArticleRegistrationNumber']) != BignummerInput:
        if BIGnummerOut not in self.in_BIGnummer:
            #print(ResultData)
            LogError(Dossieritemnummer, '\t\tVerschil tussen Bignummer in Profit en register')
            LogError(Dossieritemnummer, 'BIGnummerOut ' + BIGnummerOut)
            LogError(Dossieritemnummer, 'Profit ' + self.in_BIGnummer)
        
        # Specialisme opzoeken indien aanwezig
        Specialisme = None
        if self.RegisterResponse['Specialism']:
            #print('Aantal Specialismes =',len(ResultData['Specialism']['SpecialismExtApp1']))
            for SpecialismRegistration in self.RegisterResponse['Specialism']['SpecialismExtApp1']:
                if str(SpecialismRegistration['ArticleRegistrationNumber']) in self.in_BIGnummer:
                    SpecialismExtApp1 = SpecialismRegistration
                    Specialisme = Specialisme_code[str(SpecialismExtApp1['TypeOfSpecialismId'])]
        
        # Maatregel en laatste update-datum opzoeken indien aanwezig
        MaatregelTekst = None
        LaatsteUpdate = None
        if self.RegisterResponse['JudgmentProvision']:
            VorigeDatum = datetime(1, 1, 1, 0, 0)
            for JudgmentProvisionExtApp in self.RegisterResponse['JudgmentProvision']['JudgmentProvisionExtApp']:
                #if str(JudgmentProvisionExtApp['ArticleNumber']) in BignummerInput:
                if JudgmentProvisionExtApp['StartDate'] > VorigeDatum:
                    MaatregelTekst = JudgmentProvisionExtApp['PublicDescription']
                    LaatsteUpdate = JudgmentProvisionExtApp['StartDate'].strftime('%Y-%m-%d')
        
        # Alle gegevens samenvoegen in een dictionary
        dict_RegisterInfo = {
            'BIGachternaam' : BIGachternaam,
            'Geslacht' : Geslacht,
            'BIGnummer': BIGnummerOut,
            'BIGGeldig': BIGGeldig,
            'Ingangsdatum' : Ingangsdatum,
            'Beroepsgroep' : Beroepsgroep,
            'Specialisme' : Specialisme,
            'Einddatum' : Einddatum,
            'MaatregelTekst' : MaatregelTekst,
            'LaatsteUpdate' : LaatsteUpdate,
            }
        return dict_RegisterInfo

    @staticmethod
    def IsNameValid(dict_Profit_Names, BIGname):
        # input : dict_Profit_Names = dictionary met Achternaam, Geboortenaam en BIGAchternaam, en BIGname = string met naam in het bigregister
        # Deze functie controleert of de BIGname overeenkomt met een van de namen in het dict_Profit_Names. Hij houdt hierbij rekening met diakritische tekens.
        # Helaas worden niet alle tekens vanzelf vervangen dus moeten sommige met de hand vervangen worden (bijv. ł -->l en ç --> c). Let op: dit staat op 2 plekken in deze functie.
        register_name_ascii = BIGname
        register_name_ascii = register_name_ascii.lower()
        register_name_ascii = re.sub("ł","l", register_name_ascii)
        register_name_ascii = re.sub("ç","c", register_name_ascii)
        register_name_ascii = str(normalize('NFKD', register_name_ascii).encode('ascii', 'ignore'),'ascii')
        
        for field, name in dict_Profit_Names.items():
            if name != '':
                name_ascii = name.lower()
                name_ascii = re.sub("ł","l", name_ascii)
                name_ascii = re.sub("ç","c", name_ascii)
                name_ascii = str(normalize('NFKD', name_ascii).encode('ascii', 'ignore'),'ascii')
                if (name_ascii in register_name_ascii) or (register_name_ascii in name_ascii):
                    return True
        LogError(Dossieritemnummer, "Profit Achternaam: |"+dict_Profit_Names['Achternaam']+"|")
        LogError(Dossieritemnummer, "Profit Geboortenaam: |"+dict_Profit_Names['Geboortenaam']+"|")
        LogError(Dossieritemnummer, "Profit BIGAchternaam: |"+dict_Profit_Names['BIGAchternaam']+"|")
        LogError(Dossieritemnummer, "register_name_ascii |" + register_name_ascii+"|")
        return False



class ProfitEmployee:
    def __init__(self, Medewerkernummer, Dossieritemnummer, BIGnummer, Namen):
        self.Medewerkernummer = Medewerkernummer
        self.Dossieritemnummer = Dossieritemnummer
        self.BIGnummer = BIGnummer
        self.Namen = Namen
        
        
        if self.BIGnummer == '' or self.BIGnummer == 'Leeg':
            LogError(Dossieritemnummer, 'Geen Registratienummer ingevuld in Profit.')
            print('BIGnummer Leeg')
            df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'Goedkeuren'] = False
            df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'Reden'] = 'Geen Registratienummer ingevuld in Profit.'
            self.UpdateWorkflow(
                Goedkeuren = False,
                Reactie = 'Geen Registratienummer ingevuld in Profit.',
                )
        elif bool(re.search('\D',self.BIGnummer)):
            LogError(Dossieritemnummer, 'Geen geldig registratienummer, deze bevat tekst.')
            print('BIGnummer bevat tekst')
            df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'Goedkeuren'] = False
            df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'Reden'] = 'Geen geldig registratienummer, deze bevat tekst.'
            self.UpdateWorkflow(
                Goedkeuren = False,
                Reactie = 'Geen geldig registratienummer, deze bevat tekst.',
                )
        else:
            self.controle = BIGcontrole(
                in_BIGnummer = self.BIGnummer,
                in_dict_Namen = self.Namen,
                )
            
            df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'Goedkeuren'] = self.controle.Goedkeuren
            df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'Reden'] = self.controle.Reden
            # als controle een attribute .HeeftMaatregel heeft dan is hij tot het einde van de functie BIGcontrole.__init__() gekomen. Daarom is de informatie uit het bigregister relevant en moet dit in Profit worden geupdate.
            if hasattr(self.controle, 'HeeftMaatregel'):
                df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'HeeftMaatregel'] = self.controle.HeeftMaatregel
                self.UpdateEmployee()
            
            self.UpdateWorkflow(
                Goedkeuren = self.controle.Goedkeuren,
                Reactie = self.controle.Reden,
                )
    
    def UpdateEmployee(self):
        dict_AfasEmployee = {'AfasEmployee':{'Element':{
            '@EmId' : self.Medewerkernummer,
            'Fields':{
                'U86A16F484FD4B5963F62BA70DF76501A' : self.controle.BIGRegisterInfo['BIGnummer'], # Bignr
                'UFB71D60042380314BF18A7BCDC37DA69' : self.controle.BIGRegisterInfo['BIGnummer'], # Registratienummer
                'U7197E24E42FD5798FB7EA8307EA05FCD' : self.controle.BIGRegisterInfo['Ingangsdatum'], # Ingangsdatum
                'U8FA1EBD04DFC773A54BF8BA64CEC2EA7' : self.controle.BIGRegisterInfo['Einddatum'], # Einddatum
                'U7E250CD34097D67D310032AE64113F39' : self.controle.BIGRegisterInfo['Beroepsgroep'], # SoortRegistratie
                'U245DB24A46ADA81FBE812CABE11C81A1' : self.controle.BIGRegisterInfo['MaatregelTekst'], # MaatregelTekst
                'U648B0BF646897E7C38BD3BE0798BBE1A' : self.controle.BIGRegisterInfo['LaatsteUpdate'], # LaatsteUpdate
                'U939F8ECB439E914B3935369613ED6582' : datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), # LaatsteControle
                'U187A658F4A2657E2829723A7766DE9C1' : self.controle.BIGRegisterInfo['BIGachternaam'][:20], # Achternaam, veld is niet langer dan 20 tekens
                'U9ACCFFCC433BD41B4A2CA29B1807DB72' : self.controle.BIGRegisterInfo['Geslacht'], # Geslacht
                'U8E7340C04638C66BF1A4EF87674C1A5C' : self.controle.HeeftMaatregel, # BoolMaatregel
                'U25B20A6D4800D093ADF7AB811C39F585' : self.controle.BIGRegisterInfo['Specialisme'], # Specialisme
                            }}}}
        #print(json.dumps(dict_AfasEmployee, indent=2, ensure_ascii=False))#
        #LogError(json.dumps(dict_AfasEmployee, indent=2, ensure_ascii=False))#
        
        # Probeer Request met Connector opnieuw als het niet in 1 keer lukt. 
        number_of_attempts = 0
        while number_of_attempts < 3:
            try:
                number_of_attempts += 1
                response = requests.put(url=url_Base+"KnEmployee", json=dict_AfasEmployee, headers=Request_headers, timeout=20)
            except requests.exceptions.ReadTimeout:
                print('\t\t\t\t\t\t\t\t\tReadTimeout')
                LogError(self.Dossieritemnummer,'Readtimeout (Profit connector) UpdateEmployee')
                continue
            break
        
        if response.status_code == requests.codes.created:
            pass
        else:
            LogError(Dossieritemnummer, 'UpdateEmployee request niet geslaagd. Dit is de response:')
            LogError(Dossieritemnummer, response)
            LogError(Dossieritemnummer, 'Dit was de JSON')
            LogError(Dossieritemnummer, json.dumps(dict_AfasEmployee, indent=2, ensure_ascii=False))
    

    def UpdateWorkflow(self, Goedkeuren, Reactie):
        dict_KnWorkflow = {'KnWorkflow':{'Element':{'Fields':{
                'SbId' : self.Dossieritemnummer,
                'WfNm' : 'E73EC2C94BC543222818FA8EB45996FC',
                'TkNm' : 'A550FE414195139485A66089A4132820',
                            }}}}
        if Goedkeuren:
            dict_KnWorkflow['KnWorkflow']['Element']['Fields']['AcNm'] = '771820404B6B2552A19DD3AA4489976D'
        else:
            dict_KnWorkflow['KnWorkflow']['Element']['Fields']['AcNm'] = '40F0A91A41B29F30BD386BA3F500A2E6'
            dict_KnWorkflow['KnWorkflow']['Element']['Fields']['Tx'] = Reactie
        #print(json.dumps(dict_KnWorkflow, indent=2))
        #LogError(json.dumps(dict_KnWorkflow, indent=2))
        
        # Probeer Request met Connector of Webservice opnieuw als het niet in 1 keer lukt. 
        number_of_attempts = 0
        while number_of_attempts < 3:
            try:
                number_of_attempts += 1
                response = requests.post(url=url_Base+"KnSubjectWorkflowReaction", json=dict_KnWorkflow, headers=Request_headers, timeout=20)
            except requests.exceptions.ReadTimeout:
                print('\t\t\t\t\t\t\t\t\tReadTimeout')
                LogError(self.Dossieritemnummer,'Readtimeout (Profit connector) UpdateWorkflow')
                continue
            break
        
        if response.status_code == requests.codes.created:
            pass
        else:
            LogError(Dossieritemnummer, 'UpdateWorkflow request niet geslaagd. Dit is de response:')
            LogError(Dossieritemnummer, response)
            LogError(Dossieritemnummer, 'Dit was de JSON')
            LogError(Dossieritemnummer, json.dumps(dict_KnWorkflow, indent=2))




def CheckTransactionLog(df_Transactions):
    Summary = {}
    
    Number_of_Transactions_Collected = df_Transactions.shape[0]
    Summary['Taken opgehaald'] = Number_of_Transactions_Collected
    
    Number_of_Transactions_Processed = df_Transactions.DoneAt.notnull().sum()
    Summary['Taken afgehandeld'] = Number_of_Transactions_Processed
    
    df_Rejections = df_Transactions[df_Transactions.Goedkeuren == False]
    Number_of_Rejections = df_Rejections.shape[0]
    Summary['Taken afgekeurd'] = Number_of_Rejections
    
    df_Mention = df_Transactions[df_Transactions.HeeftMaatregel == True]
    Number_of_Mentions = df_Mention.shape[0]
    Summary['Aantal maatregelen'] = Number_of_Mentions
    
    return Summary


def Send_Report_Mail(dict_Summary):
    import smtplib, ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_server = "smtp.office365.com"
    port = 587  # For starttls
    sender_email = "me@email"
    password = "abcdefg"
    receiver = "me@email"
    receiver_cc = "boss@email"
    
    # Create a secure SSL context
    context = ssl.create_default_context()

    # Try to log in to server and send email
    try:
        server = smtplib.SMTP(smtp_server,port)
        server.ehlo() # Can be omitted
        server.starttls(context=context) # Secure the connection
        server.ehlo() # Can be omitted
        server.login(sender_email, password)
        
        message = MIMEMultipart("alternative")
        message["Subject"] = "BIG-controle overzicht"
        message["From"] = sender_email
        message["To"] = receiver
        message["Cc"] = receiver_cc
        
        # Create the plain-text and HTML version of your message
        html = f"""
        <html>
        <head>
        <style>
        table, th, td {{
        border: 1px solid black;
        border-collapse: collapse;
        }}
        </style>
        </head>
        <body>
            <p>Samenvatting BIG-controle voor Klant, gestart om {timestamp}.</p>
            <table style="width:30%">
          <tr>
            <th>Wat</th>
            <th>Aantal</th> 
          </tr>
        """
        
        for key, value in dict_Summary.items():
            html += \
            f"""
            <tr>
                <td>{key}</td>
                <td>{value}</td>
            </tr>"""
        
        
        html += \
        """</table>
        
        <p>Groeten van de Robot</p>
          </body>
        </html>
        """
        
        print(html)
        print(type(html))
        text = re.sub('<.*?>','',html)
        
        # Turn these into plain/html MIMEText objects
        MIME_text = MIMEText(text, "plain")
        MIME_html = MIMEText(html, "html")

        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        message.attach(MIME_text)
        message.attach(MIME_html)

        
        # Send email here
        print(message)
        receivers = [receiver, receiver_cc]
        server.sendmail(
            sender_email, receivers, message.as_string()
            )
        
    except Exception as e:
        # Print any error messages to stdout
        print(e)
    finally:
        server.quit()






if __name__ == '__main__':
    
    ## Hier begint het proces. Eerst worden alle taken opgehaald.
    Tasks = GetAllTasks()
    print('Aantal taken opgehaald =', len(Tasks))
    
    # CSVbestand maken met taken. Hierin wordt de voortgang bijgehouden.
    df_Transactions = CreateTransactionsCSV(Tasks)
    
    for Counter, Task in enumerate(Tasks):
        if Counter > -1: # Deze conditie doet nu niks maar kan gebruikt worden om maar een deel van de taken af te handelen.
        #if Task['DossierItemNr'] == 361058:
            Dossieritemnummer = Task['DossierItemNr']
            print(Dossieritemnummer)
            BIGnummer, Namen = Onderwerp_Uit_Elkaar_Trekken(Task['Onderwerp'])
            try:
                MDW = ProfitEmployee(
                    Medewerkernummer = Task['Medewerker'],
                    Dossieritemnummer = Dossieritemnummer,
                    BIGnummer = BIGnummer.replace(' ',''),
                    Namen = Namen,
                    )
            except Exception as exception:
                df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'Reden'] = exception
                
                ExceptionFile = 'Exception_{0}.txt'.format(datetime.now().strftime("%Y-%m-%dT%H-%M-%S"))
                with open(ExceptionFile ,'w', encoding="utf-8") as file:
                    file.write('Dossieritemnummer'+str(Dossieritemnummer) +'\n')
                    file.write(str(exception) +'\n')
                    file.write(str(type(exception)) +'\n')
                    for arg in exception.args:
                        file.write(str(arg) +'\n')
                    
                
            else:
                df_Transactions.at[df_Transactions['DossierItemNr'] == Dossieritemnummer, 'DoneAt'] = datetime.now()
            finally:
                try:
                    df_Transactions.to_csv(LogTransactionsFile, index=False, sep=';')
                except PermissionError:
                    print('PermissionError (in wegschrijven Transaction csv)')
                    sleep(5)
                    df_Transactions.to_csv(LogTransactionsFile, index=False, sep=';')
    
    
    ## Afronden van proces.
    #RemoveLogIfIrrelevant()
    # laatse keer opslaan van csv
    df_Transactions.to_csv(LogTransactionsFile, index=False, sep=';')
    
    # transactions samenvatten
    Summary_Transactions = CheckTransactionLog(df_Transactions)
    # email sturen met samenvatting
    Send_Report_Mail(Summary_Transactions)
