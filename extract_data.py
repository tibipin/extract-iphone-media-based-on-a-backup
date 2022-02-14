import plistlib
import os
import sqlite3
import pandas
import shutil
import tqdm

backup_folder_name = input('Please provide the path to the backup folder:\n')
backup_folder = os.path.join(os.getcwd(), backup_folder_name)
backup_database = os.path.join(backup_folder, 'Manifest.db')
connection = sqlite3.connect(backup_database)
c = connection.cursor()
df = pandas.read_sql('select * from files where domain="CameraRollDomain"', con=connection)
destination_folder_name = input('Please provide the path to where you want to save your media:\n')
destination_folder = os.path.join(os.getcwd(), destination_folder_name)


def extract_picture_data(fileid):
    fileblob = c.execute(f"select file from files where fileID='{fileid}'").fetchall()[0][0]
    s = plistlib.loads(fileblob)
    objects = s['$objects']
    current_file_name = os.path.join(backup_folder, fileid[:2], fileid)
    path_exists = os.path.exists(current_file_name)
    try:
        metadata = plistlib.loads(objects[3])
        original_file_name = metadata['com.apple.assetsd.originalFilename'].decode('utf-8')
        file_year = plistlib.loads(metadata['com.apple.assetsd.addedDate']).year
    except Exception:
        original_file_name = None
        file_year = None
    finally:
        return {'original_file_name': original_file_name,
                'file_year': str(file_year),
                'current_file_name': current_file_name,
                'path_exists': path_exists}


for key in {'original_file_name', 'file_year', 'current_file_name', 'path_exists'}:
    df[key] = df['fileID'].apply(lambda x: extract_picture_data(x)[key])

df = df[df['path_exists'] == True]
df = df[df['original_file_name'].isnull() == False]
df['new_file_name'] = df.apply(lambda x: fr"{destination_folder}\{str(x['file_year'])}\{x['original_file_name']}",
                               axis=1)

unique_years = [i for i in df['file_year'].unique()]
for i in unique_years:
    if not os.path.exists(os.path.join(destination_folder_name, i)):
        os.mkdir(os.path.join(destination_folder_name, i))

for i in tqdm.tqdm(df.iterrows()):
    shutil.copyfile(i[1]['current_file_name'], i[1]['new_file_name'])
