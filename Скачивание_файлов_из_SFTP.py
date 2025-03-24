from datetime import datetime
start = datetime.now()
import paramiko
import os
from pathlib import Path
from contextlib import redirect_stdout
import shutil
import sys
import subprocess
import gzip
import aspose.zip


host = '127.0.0.1'
port = int(22)
username = 'usersftp'
pw = 'usersftp'
local = '/home/atis/forsignal/1'
remote = '/upload'
transport = paramiko.Transport((host, port))
transport.connect(username=username, password=pw)
sftp = paramiko.SFTPClient.from_transport(transport)
files = sftp.listdir(remote)
for file in files:
    if file.endswith('.gz'):
        sftp.get(os.path.join(remote, file), os.path.join(local, file))
        #sftp.remove(os.path.join(remote, file))
sftp.close()
transport.close()
transport.close()


for root, dirs, files in os.walk(local):
    for file in files:
        if file.endswith('.gz'):
            archgz = os.path.join(root, file)
            outcsv = os.path.join(root, file + '.old')
            gunzip = os.system(f'zcat "{archgz}" > "{outcsv}"')
            os.remove(os.path.join(local, file))


for root, dirs, files in os.walk(local):
    for file in files:
        if file.endswith('.gz.old'):
            print(file)
            src = os.path.join(root, file)
            dst01 = Path(src).with_suffix('').with_suffix('')
            dst11 = Path(dst01).with_suffix('.workcsv')
            print("dst", dst11)
            shutil.copyfile(src, dst11)


#for root, dirs, files in os.walk(local):
#    for file in files:
#        if file.endswith('.csv1'):
#            os.path.splitext(os.path.join(root, file))[0]
#            os.rename(os.path.join(root, file), os.path.join(root, file + '.csv.old'))
            #Path(os.path.join(root, file).with_suffix('.csv.old'))
            #shutil.copy(os.path.join(root, file), os.path.join(root, file + '.csv.old'))

elapsed = datetime.now() - start
print(f"Затраченное время: {elapsed.total_seconds()} сек")
