#uploads data in azure data lake 
import pandas as pd
all_df_summer=pd.read_csv('Athletes_summer_games.csv')
all_df_winter=pd.read_csv('Athletes_winter_games.csv')
regions=pd.read_csv('regions.csv')
olympics=[all_df_summer,all_df_winter,regions]
for i in olympics:
  print(i.columns)
def merge_all():
  all_df_summer=pd.read_csv('Athletes_summer_games.csv')

  year_all=all_df_summer
  year_all.drop(columns=['Unnamed: 0','Games','Season'],inplace=True)
  year_all.rename(columns={'Sex':'Gender','Team':'Country'},inplace=True)
  year_all['Medal']=year_all['Medal'].fillna('None')
  print(f'Shape of Data year_all : {year_all.shape}')

  return year_all

final_df=merge_all()
final_df.head()
from azure.storage.filedatalake import DataLakeFileClient, DataLakeServiceClient
import pandas as pd
import os
# Connection string with your account name and key
connection_string = ''

# Specify your file system name
file_system_name = "allolympicsdataraw"
# Specify container name and file name
container_name = "rawdata"
file_name = "olympics_data.csv"

def upload_data(connection_string,file_system_name,container_name,file_name):

  # Create DataLakeStoreClient
  datalake_client = DataLakeServiceClient.from_connection_string(connection_string)


  # Create the file system if it doesn't exist
  datalake_client.create_file_system(file_system_name)


  # Assuming final_df is a pandas DataFrame
  final_df = merge_all()

  # Convert DataFrame to bytes
  file_content = final_df.to_csv(index=False).encode('utf-8')

  # Get the file client and upload the data
  file_client = datalake_client.get_file_system_client(file_system_name).get_directory_client(container_name).get_file_client(file_name)
  file_client.create_file()
  file_client.append_data(data=file_content, offset=0, length=len(file_content))
  file_client.flush_data(len(file_content))

  print('Upload Complete')
upload_data(connection_string,file_system_name,container_name,file_name)
