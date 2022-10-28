from inset_to_psg_hist import insert_to_psg
import os.path

for address, dirs, files in os.walk('file_csv/'):
    files = sorted(files)
    for name in files:
        print(address + name)
        insert_to_psg(address + name)