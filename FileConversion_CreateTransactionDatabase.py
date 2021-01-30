import pandas as pd
import sqlite3
import os
import time


def LaadKenmerkcombinaties():
    if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Kenmerkcombinaties';").fetchone():
        print('LaadKenmerkcombinaties')
        df_Kenmerkcombinaties = pd.read_excel('ProfitTabellen/PT_FRESH_Kenmerkcombinaties.xlsx', sheet_name='Kenmerkcombinaties', skiprows=3, header=0, dtype=object)
        df_Kenmerkcombinaties.drop(columns='Gbl.', inplace=True)
        df_Kenmerkcombinaties.rename(columns={
            'Type': 'Typedossieritemnummer_FIN',
            'Waarde kenmerk 1': 'Kenmerk1_FIN',
            'Waarde kenmerk 2': 'Kenmerk2_FIN',
            'Waarde kenmerk 3': 'Kenmerk3_FIN',
            }, inplace=True)
        #with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 800):  # more options can be specified also
        #    print(df_Kenmerkcombinaties)
        df_Kenmerkcombinaties.to_sql('Kenmerkcombinaties', connection)

def LaadWasWordtKenmerken():
    if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='WasWordtKenmerken';").fetchone():
        print('LaadWasWordtKenmerken')
        df_kenmerken_AA = pd.DataFrame()
        df_kenmerken_ZZ = pd.DataFrame()
        for ken in range(1,4):
            print(f'Reading file: Kernmerkwaarde incl. code_AA_{ken}.xlsx')
            df_Kenmerk_AA = pd.read_excel(f'ProfitTabellen/Kernmerkwaarde incl. code_AA_{ken}.xlsx', sheet_name='Kernmerkwaarde incl. code', dtype={'Kenmerkcode': int, 'Kenmerk': str})
            df_Kenmerk_AA.rename(columns={
                'Kenmerkcode': 'Code_HRM',
                'Kenmerk': 'Omschrijving'
                }, inplace=True)
            df_Kenmerk_AA.drop(columns='Gbl.', inplace=True)
            df_Kenmerk_AA['kenmerknummer'] = ken
            df_kenmerken_AA = df_kenmerken_AA.append(df_Kenmerk_AA)
        df_kenmerken_AA.replace({
            'Röntgen certificaat':'Röntgencertificaat',
            'CV en  sollicitiebrief':'CV en sollicitatiebrief',
            'BHV certificaat':'BHV-certificaat',
            'BIG inschrijving ':'BIG-inschrijving ',
            '-Financiele administriatie':'-Financiele administratie',
            'Overige correspondentie ':'Overige correspondentie',
            'Differentaties, aandachtsgeb., specialisatie':'Differentiaties, aandachtsgeb., specialisatie',
            '-Betref een medewerker':'-Betreft een medewerker',
            'Medewerker in dienst (praktijken)':'Medewerker indienst praktijken',
            'Medewerker uit dienst Praktijken':'Medewerker uit dienst praktijken',
            'Inschr. Reg.com. Thk Specialismen (RTS)':'Insch. Reg.com. Thk Specialismen (RTS)',
            'Opleiding en bevoegdheid ----':'Opleiding en bevoegdheid ---',
            'Overige':'Overig',
            'Registratie en Declaratie - Uitdienstmelding':'Registratie en declaratie - uitdienstmelding',
            '- Salarisdossier':'-Salarisdossier',
            'Addendum OvO continuïteitsbijdrage':'Addendum OvO Continuïteitsbijdrage',
            'Wijziging / wens':'Wijziging/wens',
            'Storing / incident':'Storing/incident',
            }, inplace=True)
        for ken in range(1,4):
            print(f'Reading file: Waarde kenmerk_AC_{ken}.xlsx')
            df_Kenmerk_ZZ = pd.read_excel(f'ProfitTabellen/Waarde kenmerk_AC_{ken}.xlsx', sheet_name='Waarde kenmerk', dtype={'Kenmerkcode': int, 'Waarde kenmerk': str})
            df_Kenmerk_ZZ.rename(columns={
                'Kenmerkcode': 'Code_FIN',
                'Waarde kenmerk': 'Omschrijving'
                }, inplace=True)
            df_Kenmerk_ZZ.drop(columns='Gbl.', inplace=True)
            df_Kenmerk_ZZ['kenmerknummer'] = ken    
            df_kenmerken_ZZ = df_kenmerken_ZZ.append(df_Kenmerk_ZZ)
        
        df_kenmerken_AA.to_sql('kenmerken_HRM', connection)
        df_kenmerken_ZZ.to_sql('kenmerken_FIN', connection)
        cursor.execute("CREATE TABLE WasWordtKenmerken(Omschrijving TEXT, Code_HRM INTEGER, Code_FIN INTEGER, Kenmerknummer INTEGER)")
        
        cursor.execute("""
            INSERT INTO WasWordtKenmerken (   Omschrijving,    Code_HRM,    Code_FIN,    Kenmerknummer)
            SELECT    kenmerken_HRM.Omschrijving,    kenmerken_HRM.Code_HRM,    kenmerken_FIN.Code_FIN,    kenmerken_HRM.kenmerknummer
            FROM kenmerken_HRM LEFT JOIN kenmerken_FIN USING(Omschrijving)
            """)
        
        ## Misschien LEFT OUTER JOIN
        
        cursor.execute("DROP TABLE kenmerken_HRM")
        cursor.execute("DROP TABLE kenmerken_FIN")
        connection.commit()

def LaadWasWordtTypes():
    if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='WasWordtTypes';").fetchone():
        print('LaadWasWordtTypes')
        df_WasWordt = pd.read_excel('ProfitTabellen/Overzicht type dossieritems en workflows.xlsx', sheet_name='Was-wordt', header=0)
        df_WasWordt.drop(columns=['Unnamed: 0','Unnamed: 3', 'Naam vrij veld', 'Veldcode', 'Datum aangemaakt', 'Opmerkingen'], inplace=True)
        df_WasWordt.rename(columns={'Nr.': 'Nr_HRM',
                                    'Nr..1': 'Nr_FIN',
                                    'Naam type dossieritem': 'Naam_typedossieritem_Was',
                                    'Naam type dossieritem.1': 'Naam_typedossieritem_Wordt'}, inplace=True)
        #df_WasWordt.Nr_FIN = df_WasWordt.Nr_FIN.fillna(0).astype(pd.Int64Dtype())
        df_WasWordt.Nr_FIN = df_WasWordt.Nr_FIN.astype(pd.Int64Dtype())
        df_WasWordt.to_sql('WasWordtTypes', connection)

def LaadWasWordtVerzuim():
    if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='WasWordtVerzuim';").fetchone():
        print('LaadWasWordtVerzuim')
        df_WasWordt = pd.read_excel('ProfitTabellen/Verzuim ID_Dummy.xlsx', sheet_name='Medewerker|verzuimmelding', header=0)
        df_WasWordt.rename(columns={'ID': 'Verzuim_HRM',
                                    'ID nieuw': 'Verzuim_FIN'}, inplace=True)
        df_WasWordt.to_sql('WasWordtVerzuim', connection)

def LaadReacties():
    if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Reacties';").fetchone():
        print('LaadReacties')
        df_Reacties = pd.read_excel('ProfitTabellen/PT_FRESH_Bestanden bij dossier.xlsx', sheet_name='Bestanden bij reactie', skiprows=3, header=0)
        df_Reacties.rename(columns={
            'Bijlage-Id': 'BijlageID',
            'Dossieritemtypenummer': 'Typedossieritemnummer',
            }, inplace=True)
        #df_Reacties.Reactie = df_Reacties.Reactie.astype(pd.Int64Dtype())
        df_Reacties.to_sql('Reacties', connection)

def LaadBijlages():
    if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Bijlages';").fetchone():
        print('LaadBijlages')
        df_Bijlages = pd.read_excel('ProfitTabellen/PT_FRESH_Bestanden bij dossier.xlsx', sheet_name='Bestanden bij dossier', skiprows=3, header=0)
        df_Bijlages.rename(columns={
            'Dossieritemtypenummer': 'Typedossieritemnummer',
            'Bijlage-Id': 'BijlageID',
            'Waarde kenmerk 1': 'Kenmerk1',
            'Waarde kenmerk 2': 'Kenmerk2',
            'Waarde kenmerk 3': 'Kenmerk3',
            }, inplace=True)
        df_Bijlages.Kenmerk1 = df_Bijlages.Kenmerk1.astype(pd.Int64Dtype())
        df_Bijlages.Kenmerk2 = df_Bijlages.Kenmerk2.astype(pd.Int64Dtype())
        df_Bijlages.Kenmerk3 = df_Bijlages.Kenmerk3.astype(pd.Int64Dtype())
        df_Bijlages.to_sql('Bijlages', connection)

def LaadDossieritems():
    if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Dossieritems';").fetchone():
        print('LaadDossieritems')
        #"""
        df_Dossieritems = pd.read_excel('ProfitTabellen/PT_FRESH_Bestanden bij dossier.xlsx', sheet_name='Dossieritems (excl. autorisatie', skiprows=3, header=0,
                                        dtype={'Werkgevernummer':pd.Int64Dtype(),
                                               'VerzuimID':pd.Int64Dtype()},
                                        parse_dates=['Instuurdatum','Datum uit Dienst', 'EinddatumDienstverband'])
        df_Dossieritems.rename(columns={
            'Typedossieritemnummer': 'Typedossieritemnummer_HRM',
            'Waarde kenmerk 1': 'Kenmerk1_HRM',
            'Waarde kenmerk 2': 'Kenmerk2_HRM',
            'Waarde kenmerk 3': 'Kenmerk3_HRM',
            'Mdw.': 'Mdw',
            'Wg.':'Wg',
            'Gebr.':'Gebr',
            'Datum uit Dienst':'DatumUitDienst',
            'VerzuimID':'VerzuimID_HRM'
            }, inplace=True)
        #print([obj for obj in list(set(df_Dossieritems.Medewerkernummer.tolist())) if not str(obj).replace('.','',1).isdigit()])
        df_Dossieritems = df_Dossieritems[~df_Dossieritems.Medewerkernummer.isin(['TGO', 'DSN', 'DMA', 'PUK', 'ASE'])]
        df_Dossieritems = df_Dossieritems[df_Dossieritems.ContractWerkgever != 'TEST']
        
        df_Dossieritems.Kenmerk1_HRM = df_Dossieritems.Kenmerk1_HRM.astype(pd.Int64Dtype())
        df_Dossieritems.Kenmerk2_HRM = df_Dossieritems.Kenmerk2_HRM.astype(pd.Int64Dtype())
        df_Dossieritems.Kenmerk3_HRM = df_Dossieritems.Kenmerk3_HRM.astype(pd.Int64Dtype())
        df_Dossieritems.Medewerkernummer = df_Dossieritems.Medewerkernummer.astype(float).astype(pd.Int64Dtype())
        df_Dossieritems.Werkgevernummer = df_Dossieritems.Werkgevernummer.astype(pd.Int64Dtype())
        df_Dossieritems.Persoonsnummer = df_Dossieritems.Persoonsnummer.astype(float).astype(pd.Int64Dtype())
        df_Dossieritems.VerzuimID_HRM = df_Dossieritems.VerzuimID_HRM.astype(pd.Int64Dtype())
        df_Dossieritems.ContractWerkgever = df_Dossieritems.ContractWerkgever.astype(float).astype(pd.Int64Dtype())
        df_Dossieritems.to_sql('Dossieritems', connection)

def AddBijlagetype():
    cursor.execute("ALTER TABLE Reacties ADD COLUMN Bijlagetype TEXT")
    cursor.execute("UPDATE Reacties SET Bijlagetype = 'Reactie'")
    cursor.execute("ALTER TABLE Bijlages ADD COLUMN Bijlagetype TEXT")
    cursor.execute("UPDATE Bijlages SET Bijlagetype = 'Bijlage'")
    connection.commit()

def AddReactieBijlageAndJoinDossieritems():
    print('AddReactieBijlageAndJoinDossieritems')
    cursor.execute("""
    CREATE TABLE TEMP_ReactieBijlage AS
        SELECT Bijlagetype, Dossieritemnummer, Reactie, BijlageID, Bijlagecode, Bestandsnaam, Bestandsgrootte FROM Reacties
        UNION ALL
        SELECT Bijlagetype, Dossieritemnummer, Null as Reactie, BijlageID, Bijlagecode, Bestandsnaam, Bestandsgrootte FROM Bijlages
    """)
    cursor.execute("""
            CREATE TABLE Transactiontable_ExtendedInfo AS
            SELECT * FROM TEMP_ReactieBijlage INNER JOIN Dossieritems
            ON TEMP_ReactieBijlage.Dossieritemnummer = Dossieritems.Dossieritemnummer
            """)
    cursor.execute("DROP TABLE TEMP_ReactieBijlage")

def FilterTransactiontable():
    print('FilterTransactiontable')
    # Verwijder:
    # Niet over te zetten typedossieritems
    cursor.execute("DELETE FROM Transactiontable_ExtendedInfo WHERE Typedossieritemnummer_HRM = 24")
    cursor.execute("DELETE FROM Transactiontable_ExtendedInfo WHERE Typedossieritemnummer_HRM = 29")
    
    # Verwijder Medewerkers die uit dienst zijn.
    # Omgekeerde selectie, verwijderd dat wat niet geselecteerd is in de binnenste SELECT
    cursor.execute("""
    DELETE FROM Transactiontable_ExtendedInfo
    WHERE RowID IN (
    Select RowID FROM Transactiontable_ExtendedInfo
    WHERE RowID NOT IN (
            SELECT RowID FROM Transactiontable_ExtendedInfo
            WHERE (
                Medewerkernummer IS NOT NULL 
                AND ContractWerkgever IS NOT NULL 
                AND (DatumUitDienst IS NULL OR DatumUitDienst >= '2020-07-01') 
                AND (EinddatumDienstverband IS NULL OR EinddatumDienstverband >= '2020-07-01')
                  )
                OR Werkgevernummer IS NOT NULL
            )
        )
    """)
    print(connection.total_changes)
    
    # Verwijder Loonstrook en Jaaropgave met bestemming Werkgever
    cursor.execute("DELETE FROM Transactiontable_ExtendedInfo WHERE Typedossieritemnummer_HRM = -2 AND Wg = 'J'")
    cursor.execute("DELETE FROM Transactiontable_ExtendedInfo WHERE Typedossieritemnummer_HRM = -3 AND Wg = 'J'")
    
    #cursor.execute("SELECT TransactionID, Bestandsnaam FROM Transactiontable WHERE Bestandsnaam REGEXP '\b.mht'") #eventueel deze eruit filteren met regex
    print(connection.total_changes)



def SortTypeKenmerk():
    print('SortTypeKenmerk')
    cursor.execute("""
    CREATE TABLE Transactiontable_ExtendedInfo_Sorted AS 
      SELECT * FROM Transactiontable_ExtendedInfo
      ORDER BY Typedossieritemnummer_HRM, Kenmerk1_HRM, Kenmerk2_HRM, Kenmerk3_HRM ASC
    """)
    cursor.execute("DROP TABLE Transactiontable_ExtendedInfo")
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo_Sorted RENAME TO Transactiontable_ExtendedInfo")

def AddTransactionID():
    print('AddTransactionID')
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN TEMP_UniekeTekst TEXT")
    cursor.execute("UPDATE Transactiontable_ExtendedInfo SET TEMP_UniekeTekst=(SELECT printf('%s_%s_%s', Dossieritemnummer, BijlageID, Medewerkernummer))")

    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN TransactionID INT")
    cursor.execute("""
            WITH cte AS (SELECT *, ROW_NUMBER() OVER() AS rn FROM Transactiontable_ExtendedInfo)
            UPDATE Transactiontable_ExtendedInfo SET TransactionID = (SELECT rn FROM cte WHERE cte.TEMP_UniekeTekst = Transactiontable_ExtendedInfo.TEMP_UniekeTekst)
            """)

def AddWasWordt():
    print('AddWasWordt')
    print('Type')
    # Typedossieritemnummer _HRM to _FIN
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN Typedossieritemnummer_FIN INT")
    cursor.execute("""
        UPDATE Transactiontable_ExtendedInfo
        SET Typedossieritemnummer_FIN = (SELECT WasWordtTypes.Nr_FIN
            FROM WasWordtTypes
            WHERE Transactiontable_ExtendedInfo.Typedossieritemnummer_HRM = WasWordtTypes.Nr_HRM)
    """)
    # Kenmerk 1 2 3 _HRM to _FIN
    print('Kenmerk 1 2 3')
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN Kenmerk1_FIN INT")
    cursor.execute("""
        UPDATE Transactiontable_ExtendedInfo
        SET Kenmerk1_FIN = (SELECT WasWordtKenmerken.Code_FIN
            FROM WasWordtKenmerken
            WHERE Transactiontable_ExtendedInfo.Kenmerk1_HRM = WasWordtKenmerken.Code_HRM)
    """)
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN Kenmerk2_FIN INT")
    cursor.execute("""
        UPDATE Transactiontable_ExtendedInfo
        SET Kenmerk2_FIN = (SELECT WasWordtKenmerken.Code_FIN
            FROM WasWordtKenmerken
            WHERE Transactiontable_ExtendedInfo.Kenmerk2_HRM = WasWordtKenmerken.Code_HRM)
    """)
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN Kenmerk3_FIN INT")
    cursor.execute("""
        UPDATE Transactiontable_ExtendedInfo
        SET Kenmerk3_FIN = (SELECT WasWordtKenmerken.Code_FIN
            FROM WasWordtKenmerken
            WHERE Transactiontable_ExtendedInfo.Kenmerk3_HRM = WasWordtKenmerken.Code_HRM)
    """)
        
    # Verzuim
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN VerzuimID_FIN INT")
    cursor.execute("""
        UPDATE Transactiontable_ExtendedInfo
        SET VerzuimID_FIN = (SELECT WasWordtVerzuim.Verzuim_FIN
            FROM WasWordtVerzuim
            WHERE Transactiontable_ExtendedInfo.VerzuimID_HRM = WasWordtVerzuim.Verzuim_HRM)
    """)
    
    # Kenmerkcombinaties
    print('Kenmerkcombinaties')
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN Kenmerkcombinatie INT")
    cursor.execute("ALTER TABLE Transactiontable_ExtendedInfo ADD COLUMN Workflow INT")
    cursor.execute("""
        UPDATE Transactiontable_ExtendedInfo
        SET (Kenmerkcombinatie, Workflow) = (SELECT Kenmerkcombinaties.Kenmerkcombinatie, Kenmerkcombinaties.Workflow
            FROM Kenmerkcombinaties WHERE
            Transactiontable_ExtendedInfo.Typedossieritemnummer_FIN = Kenmerkcombinaties.Typedossieritemnummer_FIN AND
            IFNULL(Transactiontable_ExtendedInfo.Kenmerk1_FIN,0) = IFNULL(Kenmerkcombinaties.Kenmerk1_FIN,0) AND
            IFNULL(Transactiontable_ExtendedInfo.Kenmerk2_FIN,0) = IFNULL(Kenmerkcombinaties.Kenmerk2_FIN,0) AND
            IFNULL(Transactiontable_ExtendedInfo.Kenmerk3_FIN,0) = IFNULL(Kenmerkcombinaties.Kenmerk3_FIN,0))
    """)

def CreateTransactiontable():
    print('CreateTransactiontable')
    cursor.execute("""
            CREATE TABLE Transactiontable(
                    TransactionID              INT          NOT NULL PRIMARY KEY,
                    Dossieritemnummer          INT          NOT NULL,
                    Bijlagetype                TEXT         NOT NULL,
                    Reactie                    INT                  ,
                    BijlageID                  INT                  ,
                    Bijlagecode                TEXT         NOT NULL,
                    Bestandsnaam               TEXT         NOT NULL,
                    Typedossieritemnummer_FIN  INT          NOT NULL,
                    Onderwerp                  TEXT         NOT NULL,
                    Instuurdatum               TIMESTAMP    NOT NULL,
                    Kenmerk1_FIN               INT                  ,
                    Kenmerk2_FIN               INT                  ,
                    Kenmerk3_FIN               INT                  ,
                    Medewerkernummer           INT                  ,
                    Werkgevernummer            INT                  ,
                    Persoonsnummer             INT                  ,
                    VerzuimID_FIN              INT                  ,
                    Workflow                   INT
    )""")
    cursor.execute("""
                    INSERT INTO Transactiontable SELECT
                    TransactionID,
                    Dossieritemnummer,
                    Bijlagetype,
                    Reactie,
                    BijlageID,
                    Bijlagecode,
                    Bestandsnaam,
                    Typedossieritemnummer_FIN,
                    Onderwerp,
                    Instuurdatum,
                    Kenmerk1_FIN,
                    Kenmerk2_FIN,
                    Kenmerk3_FIN,
                    Medewerkernummer,
                    Werkgevernummer,
                    Persoonsnummer,
                    VerzuimID_FIN,
                    Workflow
                    FROM Transactiontable_ExtendedInfo
                    """)
    #cursor.execute("DROP TABLE Transactiontable_ExtendedInfo")
    cursor.execute("ALTER TABLE Transactiontable ADD COLUMN Nieuw_Dossieritem INT")
    cursor.execute("ALTER TABLE Transactiontable ADD COLUMN status_code TEXT")
    cursor.execute("ALTER TABLE Transactiontable ADD COLUMN errorNumber INT")
    cursor.execute("ALTER TABLE Transactiontable ADD COLUMN externalMessage TEXT")
    cursor.execute("ALTER TABLE Transactiontable ADD COLUMN profitLogReference TEXT")





#"""
db_filename = 'ConversieData.db'

connection = sqlite3.connect(db_filename)
#connection.set_trace_callback(print)
cursor = connection.cursor()

LaadKenmerkcombinaties()
LaadWasWordtKenmerken()
LaadWasWordtTypes()
LaadWasWordtVerzuim()
LaadReacties()
LaadBijlages()
LaadDossieritems()

AddBijlagetype()
AddReactieBijlageAndJoinDossieritems()
FilterTransactiontable()
SortTypeKenmerk()
AddTransactionID()
AddWasWordt()


#cursor.close()
#connection.close()
#"""

#if not os.path.isfile('ConversieData_Test.db'):
#    os.popen('copy ConversieData.db ConversieData_Test.db')
#    time.sleep(1)

#connection = sqlite3.connect('ConversieData_Test.db')
#cursor = connection.cursor()

CreateTransactiontable()

connection.commit()
cursor.close()
connection.close()

# Na afloop uitval ophalen:
# SELECT * FROM TransactionTable WHERE Nieuw_Dossieritem IS NULL
