#%%
import fiona
import geopandas as gpd
import requests
import zipfile
from pathlib import Path
import pandas as pd
# %%
def download_file(url, local_filename):
    # Send a HTTP request to the given URL
    response = requests.get(url, stream=True)

    # Check if the request was successful
    if response.status_code == 200:
        # Open the local file for writing in binary mode
        with open(local_filename, 'wb') as f:
            # Iterate over the response content and write it to the file
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    f.write(chunk)
        print(f"File downloaded successfully: {local_filename}")
    else:
        print(f"Failed to download file. HTTP Status Code: {response.status_code}")

def unzip_file(zip_filepath, extract_to='.'):
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        print(f"File unzipped successfully to {extract_to}")


#%%
download_zip_file = Path('/tmp/data.zip')
url = 'https://nedlasting.miljodirektoratet.no/naturovervaking/naturovervaking_eksport.gdb.zip'
download_file(url, download_zip_file)
# %%
gdb_file_dir = Path('/tmp/data')
unzip_file(download_zip_file, gdb_file_dir)
# %%
gdb_file = next(gdb_file_dir.glob('*.gdb'))
# %%
# List all layers (tables) in the GDB file
layers = fiona.listlayers(gdb_file)
# Dictionary to store each layer as a GeoDataFrame
gdb_data = {}

for layer in layers:
    gdb_data[layer] = gpd.read_file(gdb_file, layer=layer)

# %%
gdb_data
# %%
# ['ANO_Problemart',
#  'ANO_Treslag',
#  'ANO_SurveyPoint',
#  'ANO_FremmedArt',
#  'ANO_Flate',
#  'ANO_Art']

layers_to_concat = ['ANO_Art', 'ANO_FremmedArt', 'ANO_Treslag', 'ANO_Problemart']  # replace with your layer names

# Collect GeoDataFrames for the specified layers
gdfs_to_concat = [gdb_data[layer] for layer in layers_to_concat]
# %%
occurrences = gpd.GeoDataFrame(pd.concat(gdfs_to_concat, ignore_index=True))
# %%
occ_mapping = {'GlobalID':'occurrenceID',
               'art_navn':'scientificName',
               'art_norsk_navn':'vernacularName',
            #    'art_dekning',
            #    'Creator':'',
               'CreationDate': 'eventDate',
            #    'Editor':'',
               'EditDate':'modified',
               'ParentGlobalID':'eventID',
            #    'geometry',
               }
# %%
occurrences.columns = [occ_mapping.get(col, col) for col in occurrences.columns]
# %%
occurrences.columns
# %%
emof_occ = occurrences.loc[:, ('occurrenceID', 'art_dekning')]
# %%
emof_occ.columns = ['occurrenceID', 'measurementValue']
# %%
emof_occ['measurementType'] = 'percent cover'
emof_occ['measurementUnit'] = '%'
emof_occ['measurementMethod'] = 'visual estimation'
emof_occ['measurementRemarks'] = 'Estimated visually over a 1m² plot'
# %%
emof_occ.dropna(how='any', inplace=True)
# %%
