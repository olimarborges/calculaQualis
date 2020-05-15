from __future__ import print_function
import pickle
import os.path
import re
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from datetime import datetime
import urllib3
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from progress.bar import Bar

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1yvuCa__L7r0EJy6v6Jb17fvu-VdV80PbfAReR9Gy52I'
SAMPLE_RANGE_NAME = 'Qualis!A1:J1572'

SERVICE_ACCOUNT_FILE = 'credencialContaServico.json'

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

def main():
    print('1/6. Iniciando o processo de configuração e autenticação com o Google Sheets, para buscar TODOS os dados da planilha.')
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """

    delegated_credentials = credentials.with_subject('[credencial_omitida]')
    service = build('sheets', 'v4', credentials=delegated_credentials)
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()

    header = result.get('values', [])[0]   # Assumes first line is header!
    values = result.get('values', [])[1:]  # Everything else is data.

    if not values:
        print('No data found.')
    else:
        all_data = []
        for col_id, col_name in enumerate(header):
            column_data = []
            for row in values:
                column_data.append(row[col_id])
            ds = pd.Series(data=column_data, name=col_name)
            all_data.append(ds)
        df = pd.concat(all_data, axis=1)
    print('Finalizou: 1/6')

    print('2/6. Iniciando o processo de busca do Qualis...')
    valores = realizaParanaue(df)
    print('Finalizou: 2/6')

    print('3/6. Iniciando o processo de configuração e autenticação novamente com o Google Sheets, para buscar ALGUNS dados da planilha para fazer o UPDATE.')
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    print('Finalizou: 3/6')

    print('4/6. Iniciando a configuração para realizar o Update na planilha.')
    #print(valores)

    # The A1 notation of the values to update. 'Qualis!F2:G2'
    range_update = 'Qualis!F2:J1572'  # TODO: Update placeholder value.

    # How the input data should be interpreted.
    value_input_option = 'USER_ENTERED'  # TODO: Update placeholder value.

    value_range_body = {
        "values": valores
    }
    #print(value_range_body)
    print('Finalizou: 4/6')
    print('5/6. Iniciando ação para efetivar o update na planilha.')

    request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=range_update,
                                    valueInputOption=value_input_option,
                                    body=value_range_body).execute()

    print('Finalizou: 5/6')
    print('6/6. Planilha atualizada com sucesso!')
    print('{0} cells updated.'.format(request.get('updatedCells')))


def realizaParanaue(df):
    qualis = None
    estratoBase = None
    valores = []
    row = df.shape[0]

    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

    with Bar('Processando', max=row) as bar:
        for index in range(0, row):
            linha = []
            #toda vez que o script executar, ele vai registrar a data desta tentativa de atualização
            linha.insert(3, dt_string)
            if df.loc[index, 'categoria'] != '3' or df.loc[index, 'categoria'] != '4':
                site = df.loc[index, 'link']
                if site != 'nulo':
                    #print(df.loc[index, 'sigla'])
                    h5, log = buscaH5(site)
                    if log != '': #deu erro ao acessar o site
                        print('_log: ', log)
                        linha.insert(0, df.loc[index, 'h5'])
                        linha.insert(1, df.loc[index, 'Qualis_Final'])
                        linha.insert(2, df.loc[index, 'data-atualizacao'])
                        linha.insert(4, log)
                    elif h5 != None:
                        print('_h5: ', h5)
                        linha.insert(0, h5)
                        linha.insert(2, dt_string)
                        if h5 != 0 :
                            estratoBase = aplicaRegra(h5)
                            if df.loc[index, 'categoria'] == '1':
                                qualis = estratoBase
                                #atualiza valor na célula do qualis desta linha
                                linha.insert(1, qualis)
                            elif df.loc[0, 'categoria'] == '2':
                                valorTop = df.loc[index, 'CE Indicou']
                                qualis = validaCategoria2(valorTop, estratoBase)
                                linha.insert(1, qualis)
                                linha.insert(4, 'atualizado com sucesso')
                            else:
                                linha.insert(1, df.loc[index, 'Qualis_Final'])
                                linha.insert(4, 'categoria inválida. Verificar.')
                        else:
                            linha.insert(1, df.loc[index, 'Qualis_Final'])
                            linha.insert(4, 'h5 é zero')
                    else:
                        linha.insert(0, df.loc[index, 'h5'])
                        linha.insert(1, df.loc[index, 'Qualis_Final'])
                        linha.insert(2, df.loc[index, 'data-atualizacao'])
                        linha.insert(4, 'h5 inválido no site')
                else:
                    linha.insert(0, df.loc[index, 'h5'])
                    linha.insert(1, df.loc[index, 'Qualis_Final'])
                    linha.insert(2, df.loc[index, 'data-atualizacao'])
                    linha.insert(4, 'não existe site para verificar o h5')
            else:
                linha.insert(0, df.loc[index, 'h5'])
                linha.insert(1, df.loc[index, 'Qualis_Final'])
                linha.insert(2, df.loc[index, 'data-atualizacao'])
                linha.insert(4, 'categorias 3 e 4 não são atualizadas')

            valores.insert(index, linha)
            bar.next()

    return valores

# executa busca no google
def buscaH5(site):
    h5index = None
    log = ''
    http = urllib3.PoolManager()
    try:
        response = http.request('GET', site)
    except urllib3.exceptions.HTTPError as e:
        #print(e)
        log = 'link do site inacessível: ' + str(e)
    else:
        res = BeautifulSoup(response.data, 'html.parser')
        tags = res.findAll("ul", {"class": "gsc_mlhd_list"}, "span")
        try:
            h5index = tags[0].span
        except IndexError as a:
            log = 'não encontrou tag span: ' + str(a)
    #deixa somente o número, tirando as tags html
    h5index = re.sub('[^0-9]', '', str(h5index))

    if h5index == '':
        h5index = None
    else:
        #base 10 transforma o valor numérico da str em decimal
        h5index = int(h5index, 10)

    return h5index, str(log)

def aplicaRegra(h5):
    estratoBase = None
    if h5 >= 35:
        estratoBase = "A1"
    elif h5 >= 25:
        estratoBase = "A2"
    elif h5 >= 20:
        estratoBase = "A3"
    elif h5 >= 15:
        estratoBase = "A4"
    elif h5 >= 12:
        estratoBase = "B1"
    elif h5 >= 9:
        estratoBase = "B2"
    elif h5 >= 6:
        estratoBase = "B3"
    elif h5 > 0:
        estratoBase = "B4"
    return estratoBase

def validaCategoria2(valorTop, estratoBase):
    qualis = None
    if valorTop == "Relevante":
        qualis = estratoBase
    elif valorTop == "Top10" or valorTop == "Top20":
        if estratoBase == "A1" or estratoBase == "A2" or estratoBase == "A3":
            qualis = estratoBase
    elif valorTop == "Top10":
        qualis = sobeNivel(estratoBase, 2)
    elif valorTop == "Top20":
        qualis = sobeNivel(estratoBase, 1)

    return qualis

def sobeNivel(estratoBase, nivel):
    qualis = None
    if nivel == 1:
        if estratoBase == "A4":
            qualis = "A3"
        elif estratoBase == "B1":
            qualis = "A4"
        elif estratoBase == "B2":
            qualis = "B1"
        elif estratoBase == "B3":
            qualis = "B2"
        elif estratoBase == "B4":
            qualis = "B3"
    elif nivel == 2:
        if estratoBase == "A4":
            qualis = "A3"
        elif estratoBase == "B1":
            qualis = "A3"
        elif estratoBase == "B2":
            qualis = "A4"
        elif estratoBase == "B3":
            qualis = "B1"
        elif estratoBase == "B4":
            qualis = "B2"

    return qualis

if __name__ == '__main__':
    main()
