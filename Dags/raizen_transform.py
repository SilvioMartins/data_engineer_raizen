###########################################################
# OBJETIVO:  Classe para transformação de dados da PIVOT
#           para arquivo .parquet
# DATA: 06Abr2022
# OWNER: Silvio Martins
###########################################################

#Importanndo Bibliotecas
import pandas as pd
import fastparquet

#Class Base_transform
class Data_transform:
    def __init__(self):
        self._file_orig = None
        self._sheet = None
        self._file_dest = None

    def set_file_orig(self,file_orig):
        self._file_orig = file_orig

    def get_file_orig(self):
        return self._file_orig
    
    def set_sheet(self, sheet):
        self._sheet = sheet

    def get_sheet(self):
        return self._sheet
    
    def set_file_dest(self,file_dest):
        self._file_dest = file_dest

    def get_file_dest(self):
        return self._file_dest

    #extrai dados da Pivot Table para um dataframe e salva como parquet particioando
    def create_df(self):
        #Dicionário de apoio de Estados
        dic_uf = {
            'ACRE':'AC',
            'AMAZONAS':'AM',
            'RORAIMA':'RR',
            'PARÁ':'PA',
            'AMAPÁ':'AP',
            'TOCANTINS':'TO',
            'MARANHÃO':'MA',
            'PIAUÍ':'PI',
            'CEARÁ':'CE',
            'RIO GRANDE DO NORTE':'RN',
            'PARAÍBA':'PB',
            'PERNAMBUCO':'PE',
            'ALAGOAS':'AL',
            'SERGIPE':'SE',
            'BAHIA':'BA',
            'MINAS GERAIS':'MG',
            'ESPÍRITO SANTO':'ES',
            'RIO DE JANEIRO':'RJ',
            'SÃO PAULO':'SP',
            'PARANÁ':'PR',
            'SANTA CATARINA':'SC',
            'RIO GRANDE DO SUL':'RS',
            'MATO GROSSO DO SUL':'MS',
            'MATO GROSSO':'MT',
            'GOIÁS':'GO',
            'DISTRITO FEDERAL':'DF',
            'RONDÔNIA':'RO'
        }

        #Dicionário de apoio de Meses
        dic_meses = {
            'Jan':'01',
            'Fev':'02',
            'Mar':'03',
            'Abr':'04',
            'Mai':'05',
            'Jun':'06',
            'Jul':'07',
            'Ago':'08',
            'Set':'09',
            'Out':'10',
            'Nov':'11',
            'Dez':'12'
        }
        
        #Lendo a sheet escolhida do xls para transformação
        df_in = pd.read_excel(self._file_orig,sheet_name=self._sheet)

        #Tratando dados Na
        df_in.fillna('0.0',inplace=True)

        #Construindo o Dataframe Final
        df_final = pd.DataFrame(columns=['year_month','uf','product','unit','volume','created_at'])
        df_final = df_final.astype({'year_month':'string',
                                    'uf':'string',
                                    'product':'string',
                                    'unit':'string',
                                    'volume':'float',
                                    'created_at':'datetime64'
                                })
        
        #Gerando Dataframe Final com base no Dataframe de Entrada(.xls)
        #Percorre cada Indice do Dataframe de Entrada
        for ind1 in df_in.index:
            #Percorre as colunas
            for ind2,col in enumerate(list(df_in)):
                #Itera só nas colunas referidas ao valores dos meses
                if ind2>3 and ind2<16:
                    #Monta a Linha de dados para o Dataframe Final
                    df_row = [
                        str(df_in['ANO'][ind1])+'-'+dic_meses[col],
                        dic_uf[df_in['ESTADO'][ind1]],
                        df_in['COMBUSTÍVEL'][ind1],
                        df_in['REGIÃO'][ind1],
                        float(df_in[col][ind1]),
                        pd.to_datetime('today')
                    ]
                    #Insere Linha de dados no Dataframe Final
                    df_final.loc[-1] = df_row 
                    df_final.index = df_final.index + 1
        #Ordena o indice do Dataframe Final
        df_final = df_final.sort_index()

        #Gera Parquet Final
        df_final.to_parquet(self._file_dest, compression='snappy', partition_cols=['uf','product'])

        #Checa os dados de entrada e de saída, caso sejam diferentes gera excessão
        #Lendo Parquet Gravado
        df_parquet = pd.read_parquet(self._file_dest)
        #Checando as quantidades de registros
        if not (df_parquet['year_month'].count() == df_final['year_month'].count()):
            raise NameError('Erro Checagem de dados: {0}'.format(self._file_dest))
        

#Função para transfromação de dados   
def process_transform(file_orig, sheet, file_dest):
    #Instancia a Classe
    proc_transform = Data_transform()
    #Incializada propriedades
    proc_transform.set_file_orig(file_orig)
    proc_transform.set_sheet(sheet)  
    proc_transform.set_file_dest(file_dest)
    #Chama Método de transformação
    proc_transform.create_df()