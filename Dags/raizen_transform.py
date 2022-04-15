###########################################################
# OBJETIVO:  Classe para transformação de dados da PIVOT
#           para arquivo .parquet
# DATA: 06Abr2022
# OWNER: Silvio Martins
###########################################################

#Importanndo Bibliotecas
import win32com.client as win32
import pandas as pd
from datetime import datetime

#Class Base_transform
class Data_transform:
    def __init__(self):
        self._path_file = None
        self._tables = None

    def set_path_file(self, path_file):
        self._file_name = path_file

    def get_path_file(self):
        return self._path_file
    
    def set_tables(self, tables):
        self._tables = tables

    def get_tables(self):
        return self._tables

    #extrai dados da Pivot Table para um dataframe e salva como parquet particioando
    def create_df(self):
        #Ajusta os Filtros
        def set_Filter(pivot_table, filter_Name, value):
            pivot_table.PivotFields(filter_Name).ClearAllFilters()
            try:
                pivot_table.PivotFields(filter_Name).CurrentPage = str(value.Name)
            except:
                for item in pivot_table.PivotFields(filter_Name).PivotItems():
                    if item.Name == value.Name:
                        item.Visible = True
                    else:
                        item.Visible = False

        #Lê o xlsx 
        DFrame = pd.DataFrame()
        tabela = win32.gencache.EnsureDispatch('Excel.Application')
        tabela.Visible = True
        wb = tabela.Workbooks.Open(self._path_file)

        #Pega os dados e coloca em dataframe
        for table in self._tables:
            pvtTable = wb.Sheets("Plan1").Range(table).PivotTable
            
            for uf in pvtTable.PivotFields("UN. DA FEDERAÇÃO").PivotItems():  
                set_Filter(pvtTable, "UN. DA FEDERAÇÃO", uf)
                
                for product in pvtTable.PivotFields("PRODUTO").PivotItems():
                    set_Filter(pvtTable, "PRODUTO", product)
                    
                    help_array = []
                    complete_array = []
                    current_line = pvtTable.TableRange1[0].Row
                    
                    for item in pvtTable.TableRange1:
                        if item.Row == current_line:
                            help_array.append (item.Value)
                        else:
                            complete_array.append (help_array)
                            help_array = [];
                            help_array.append (item.Value)
                        
                        current_line = item.Row
                    
                    for year in complete_array[1][1:]:
                        for i in range(2,len(complete_array),1):
                            DFrame = DFrame.append (pd.DataFrame({'year_month':[datetime(int(year), int(i-1), 1)],'uf': [str(uf)], 'product': [str(product)], 'unit' : ['m3'], 'volume' : [complete_array[i][complete_array[1].index(year)]], 'created_at' : [datetime.today()]}))

        #Salva o dataframe como .parquet       
        DFrame=DFrame.astype({'uf':str,'product':str,'unit':str,'volume':float})        
        return DFrame

#Função para transfromação de dados   
def Dframe_final(self, path_file_xlsx):
    dataframe_creating = Data_transform()
    dataframe_creating.set_path_file(path_file_xlsx)
    dataframe_creating.set_tables(["B52","B132"])  
    df_final = dataframe_creating.create_df()
    df_final.to_parquet('./products_fuel.parquet', compression='snappy', partition_cols=['uf','product']) 
 