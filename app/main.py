#%%
import fiona
import geopandas as gpd
import numpy as np
import requests
import zipfile
from pathlib import Path
import pandas as pd
import json
import requests
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
#%%
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
# ['ANO_Problemart',
#  'ANO_Treslag',
#  'ANO_SurveyPoint',
#  'ANO_FremmedArt',
#  'ANO_Flate',
#  'ANO_Art']
#%%
gdb_data['ANO_Flate']
event_flate = gdb_data['ANO_Flate']
event_flate_mapping = {
    'ano_flate_id': 'eventID',
    'ssb_id': 'locationID',
    'geometry': 'footprintWKT'
}
event_flate.columns = map(lambda x: event_flate_mapping[x], list(event_flate.columns))
#%%
event_flate['locationID'] = event_flate['locationID'].apply(lambda x: f'ssb:{x}')
#%%
event_points = gdb_data['ANO_SurveyPoint']
#%% Survey Points mapping
event_points_mapping = {'GlobalID': 'eventID',
'registeringsdato': 'eventDate',
# 'klokkeslett_start': 'eventTime',
'ano_flate_id': 'parentEventID',
'ano_punkt_id': 'locationID',
# 'ssb_id', Drop because its redundant in Flate
# 'program', Drop because defined in metadata
'instruks': 'samplingProtocol',
'aar': 'year',
# 'dataansvarlig_mdir',  Add to dynamic properties?
'dataeier': 'rightsHolder',
'vaer': 'locationRemarks',
# 'hovedoekosystem_250m2', # Add to MOF
# 'andel_hovedoekosystem_250m2', # Add to MOF
# 'utilgjengelig_punkt',  Add to dynamic properties
# 'utilgjengelig_begrunnelse',  Add to dynamic properties
# 'gps': 'georeferenceRemarks', # Merge to georeferenceRemarks
# 'noeyaktighet': 'coordinateUncertaintyInMeters', # needs adujustment
# 'kommentar_posisjon': 'georeferenceRemarks',
# 'klokkeslett_karplanter_start', already in klokkeslett_start
# 'karplanter_dekning', # Add to MOF
# 'klokkeslett_karplanter_slutt', Drop
# 'karplanter_feltsjikt', # Add to MOF
# 'moser_dekning', # Add to MOF
# 'torvmoser_dekning', # Add to MOF
# 'lav_dekning', # Add to MOF
# 'stroe_dekning', # Add to MOF
# 'jord_grus_stein_berg_dekning', # Add to MOF
# 'stubber_kvister_dekning', # Add to MOF
# 'alger_fjell_dekning', # Add to MOF
'kommentar_ruteanalyse': 'eventRemarks',
# 'fastmerker', Add to dynamicProperties
# 'kommentar_fastmerker', Add to dynamicProperties
# 'kartleggingsenhet_1m2', # Add to MOF
# 'hovedtype_1m2', # Add to MOF
# 'ke_beskrivelse_1m2', # Add to MOF
# 'kartleggingsenhet_250m2', # Add to MOF
# 'hovedtype_250m2', # Add to MOF
# 'ke_beskrivelse_250m2', # Add to MOF
# 'andel_kartleggingsenhet_250m2', # Add to MOF
# 'bv_7gr_gi', # Add to MOF
# 'bv_7jb_ba', # Add to MOF
# 'bv_7jb_bt', # Add to MOF
# 'bv_7jb_si', # Add to MOF
# 'bv_7tk', # Add to MOF
# 'bv_7se', # Add to MOF
# 'forekomst_ntyp', # Probably drop - ask because it contains ['nei', None, 'ja', '']
# 'ntyp', # Add to MOF
# 'kommentar_naturtyperegistering', # Add to MOF as comment
# 'krypende_vier_dekning', # Add to MOF
# 'ikke_krypende_vier_dekning', # Add to MOF
# 'vedplanter_total_dekning', # Add to MOF
# 'busker_dekning', # Add to MOF
# 'tresjikt_dekning', # Add to MOF
# 'roesslyng_dekning', # Add to MOF
# 'roesslyngblad', # Add to MOF
# 'problemarter_dekning', # Add to MOF
# 'problemarter_kommentar', # Add to MOF as a comment
# 'fremmedarter_total_dekning', # Add to MOF
# 'kommentar_250m2_flate', # Add to MOF as a comment
# 'klokkeslett_slutt', # Drop
# 'faktaark_url', Drop because 404
# 'vedlegg_url', # Since we can't add images to event directly, we acc it to dynamicProperty
'creator': 'identifiedBy',
# 'creationdate', Drop because irrelevant
# 'editor', Drop?
'editdate': 'modified',
'geometry': 'footprintWKT'}

def get_dynamic_properties(row):
    properties = ['utilgjengelig_punkt', 'utilgjengelig_begrunnelse', 'fastmerker', 'kommentar_fastmerker', 'vedlegg_url']
    dyn_prop_map = {x : row[x] for x in properties if row[x] != None}
    if dyn_prop_map == {}:
        return None
    return json.dumps(dyn_prop_map)

event_points['dynamicProperties'] = event_points.apply(get_dynamic_properties, axis=1)
#%%
def get_event_mof(row):
    mofs = [
        'hovedoekosystem_250m2',
        'andel_hovedoekosystem_250m2',
        'karplanter_dekning',
        'karplanter_feltsjikt',
        'moser_dekning',
        'torvmoser_dekning',
        'lav_dekning',
        'stroe_dekning',
        'jord_grus_stein_berg_dekning',
        'stubber_kvister_dekning',
        'alger_fjell_dekning',
        'kartleggingsenhet_1m2',
        'hovedtype_1m2',
        'ke_beskrivelse_1m2',
        'kartleggingsenhet_250m2',
        'hovedtype_250m2',
        'ke_beskrivelse_250m2',
        'andel_kartleggingsenhet_250m2',
        'bv_7gr_gi',
        'bv_7jb_ba',
        'bv_7jb_bt',
        'bv_7jb_si',
        'bv_7tk',
        'bv_7se',
        'ntyp',
        'krypende_vier_dekning',
        'ikke_krypende_vier_dekning',
        'vedplanter_total_dekning',
        'busker_dekning',
        'tresjikt_dekning',
        'roesslyng_dekning',
        'roesslyngblad',
        'problemarter_dekning',
        'fremmedarter_total_dekning'
    ]
    def ignore(var):
        if var in [None, '', {}]:
            return True
        try:
            return np.isnan(var)
        except:
            pass
        return False

    mofs_map = [{'measurementID': f"{row['GlobalID']}:mof:{x}", 'eventID': row['GlobalID'], 'measurementType': x, 'measurementValue': row[x]} for x in mofs if not ignore(row[x])]
    mofs_df = pd.DataFrame(mofs_map)

    def add_measurement_unit(mof):
        if 'dekning' in mof['measurementType']:
            return '%'
        else:
            return None
    mofs_df['measurementUnit'] = mofs_df.apply(add_measurement_unit, axis=1)

    mofs_df['measurementRemarks'] = None
    if row['kommentar_naturtyperegistering'] != None:
        mofs_df.loc[mofs_df['measurementType'] == 'kartleggingsenhet_1m2', 'measurementRemarks'] = row['kommentar_naturtyperegistering']
    if row['problemarter_kommentar'] != None:
        mofs_df.loc[mofs_df['measurementType'] == 'problemarter_dekning', 'measurementRemarks'] = row['problemarter_kommentar']
    if row['kommentar_250m2_flate'] != None:
        mofs_df.loc[mofs_df['measurementType'] == 'hovedoekosystem_250m2', 'measurementRemarks'] = row['kommentar_250m2_flate']
    return mofs_df


#%%
event_points['mofs'] = event_points.apply(get_event_mof, axis=1)
#%%
mofs = pd.concat(list(event_points['mofs']))
#%%
event_points['georeferenceRemarks'] = event_points.apply(lambda x: f"{event_points['gps']} | {event_points['kommentar_posisjon']}")
#%%
def get_uncertainity(row):
    uncertanity_map = {
        None: None,
        '1-5cm': 0.05,
        '5-9cm': 0.09,
        '0,5-1m': 1.0,
        '10-19cm': 0.19,
        '40-50cm': 0.5,
        '1-5m': 5.0,
        '20-29cm': 0.29,
        '30-39cm': 0.39,
        '': None,
        '5-10m': 10.0,
        '>10m': 10.0,
        '<0,2m': 0.2,
        '0,2-1m': 1}
    return uncertanity_map[row['noeyaktighet']]
event_points['coordinateUncertaintyInMeters'] = event_points.apply(get_uncertainity, axis=1)

#%%
event_points_columns_to_drop = [
    'klokkeslett_start',
    'ssb_id',
    'program',
    'dataansvarlig_mdir',
    'hovedoekosystem_250m2',
    'andel_hovedoekosystem_250m2',
    'utilgjengelig_punkt',
    'utilgjengelig_begrunnelse',
    'gps',
    'noeyaktighet',
    'kommentar_posisjon',
    'klokkeslett_karplanter_start',
    'karplanter_dekning',
    'klokkeslett_karplanter_slutt',
    'karplanter_feltsjikt',
    'moser_dekning',
    'torvmoser_dekning',
    'lav_dekning',
    'stroe_dekning',
    'jord_grus_stein_berg_dekning',
    'stubber_kvister_dekning',
    'alger_fjell_dekning',
    'fastmerker',
    'kommentar_fastmerker',
    'kartleggingsenhet_1m2',
    'hovedtype_1m2',
    'ke_beskrivelse_1m2',
    'kartleggingsenhet_250m2',
    'hovedtype_250m2',
    'ke_beskrivelse_250m2',
    'andel_kartleggingsenhet_250m2',
    'bv_7gr_gi',
    'bv_7jb_ba',
    'bv_7jb_bt',
    'bv_7jb_si',
    'bv_7tk',
    'bv_7se',
    'forekomst_ntyp',
    'ntyp',
    'kommentar_naturtyperegistering',
    'krypende_vier_dekning',
    'ikke_krypende_vier_dekning',
    'vedplanter_total_dekning',
    'busker_dekning',
    'tresjikt_dekning',
    'roesslyng_dekning',
    'roesslyngblad',
    'problemarter_dekning',
    'problemarter_kommentar',
    'fremmedarter_total_dekning',
    'kommentar_250m2_flate',
    'klokkeslett_slutt',
    'faktaark_url',
    'vedlegg_url',
    'creationdate',
    'editor',
    'mofs']
event_points.drop(columns=event_points_columns_to_drop, inplace=True)
#%%
def map_event_points_column(col_name):
    if col_name in event_points_mapping.keys():
        return event_points_mapping[col_name]
    return col_name

event_points.columns = [map_event_points_column(x) for x in event_points.columns]

#%%
event_points['footprintSRS'] = 'EPSG:25833'
#%%
event_points['decimalLatitude'] = event_points['footprintWKT'].apply(lambda x: x.coords.xy[0][0])
event_points['decimalLongitude'] = event_points['footprintWKT'].apply(lambda x: x.coords.xy[1][0])
event_points['geodeticDatum'] = 'EPSG:25833'
#%% Art
event_art = event_points.loc[:, ['eventID', 'eventDate', 'locationID', 'samplingProtocol', 'year', 'rightsHolder', 'locationRemarks', 'eventRemarks', 'identifiedBy', 'modified', 'footprintWKT', 'dynamicProperties', 'georeferenceRemarks', 'coordinateUncertaintyInMeters', 'footprintSRS', 'decimalLatitude', 'decimalLongitude', 'geodeticDatum']]
event_art['parentEventID'] = event_art['eventID']
event_art['eventID'] = event_art['eventID'].apply(lambda x: f'{x}:Art')
#%%
occ_art = gdb_data['ANO_Art']
#%%
occ_art_mapping = {
'GlobalID' : 'occurrenceID',
'art_navn' : 'scientificName',
'art_norsk_navn': 'vernacularName',
'art_dekning': 'organismQuantity',
'Creator': 'recordedBy',
'CreationDate': 'dateIdentified',
# 'Editor': ,
# 'EditDate',
'ParentGlobalID': 'eventID',
# 'geometry'
}
occ_art.drop(columns=['Editor', 'EditDate', 'geometry'], inplace=True)
#%%
occ_art.columns = [occ_art_mapping[x] for x in occ_art.columns]
#%%
occ_art['organismQuantityType'] = '% biomass'
occ_art['eventID'] = occ_art['eventID'].apply(lambda x: f'{x}:occ:Art')
#%%
def process_event_and_occurrence(event_points, gdb_data, suffix, layer):
    """
    Process event and occurrence data with a specified suffix and layer.

    Parameters:
        event_points (pd.DataFrame): The DataFrame containing event points.
        gdb_data (dict): The dictionary containing different layers of data.
        suffix (str): The suffix to be added to the eventID.
        layer (str): The specific layer in gdb_data to be processed.

    Returns:
        event_df (pd.DataFrame): The processed event DataFrame.
        occurrence_df (pd.DataFrame): The processed occurrence DataFrame.
    """

    # Process event data
    event_df = event_points.loc[:, ['eventID', 'eventDate', 'locationID', 'samplingProtocol', 'year', 'rightsHolder', 'locationRemarks', 'eventRemarks', 'identifiedBy', 'modified', 'footprintWKT', 'dynamicProperties', 'georeferenceRemarks', 'coordinateUncertaintyInMeters', 'footprintSRS', 'decimalLatitude', 'decimalLongitude', 'geodeticDatum']]
    event_df['parentEventID'] = event_df['eventID']
    event_df['eventID'] = event_df['eventID'].apply(lambda x: f'{x}:occ:{suffix}')

    # Process occurrence data
    occurrence_df = gdb_data[layer]
    occurrence_mapping = {
        'GlobalID' : 'occurrenceID',
        'art_navn' : 'scientificName',
        'art_norsk_navn': 'vernacularName',
        'Creator': 'recordedBy',
        'CreationDate': 'dateIdentified',
        # 'Editor': ,
        # 'EditDate',
        'ParentGlobalID': 'eventID',
        # 'geometry'
    }

    # Drop columns that are not needed
    columns_to_drop = ['Editor', 'EditDate', 'geometry']
    columns_to_drop = [col for col in columns_to_drop if col in occurrence_df.columns]
    occurrence_df.drop(columns=columns_to_drop, inplace=True)

    # Rename columns according to the mapping
    occurrence_df.columns = [occurrence_mapping.get(col, col) for col in occurrence_df.columns]

    # Update eventID with suffix
    occurrence_df['eventID'] = occurrence_df['eventID'].apply(lambda x: f'{x}:{suffix}')

    return event_df, occurrence_df

event_fremmed_art, occ_fremmed_art = process_event_and_occurrence(event_points, gdb_data, 'FremmedArt', 'ANO_FremmedArt')
event_treslag, occ_treslag = process_event_and_occurrence(event_points, gdb_data, 'Treslag', 'ANO_Treslag')
event_problemart, occ_problemart = process_event_and_occurrence(event_points, gdb_data, 'Problemart', 'ANO_Problemart')

#%% Finalise
event_all = pd.concat([event_flate, event_points, event_art, event_fremmed_art, event_treslag, event_problemart])
# %%
occ_all = pd.concat([occ_art, occ_fremmed_art, occ_treslag, occ_problemart])
# %% Save
occ_all.to_excel('/output/occurence.xlsx', index=False)
event_all.to_excel('/output/event.xlsx', index=False)
mofs.to_excel('/output/mof.xlsx', index=False)
