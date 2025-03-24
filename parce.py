from datetime import datetime
start = datetime.now()
import paramiko
import os
from pathlib import Path
import shutil
import pandas as pd
import numpy as np
import re

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
        sftp.remove(os.path.join(remote, file))
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
            #print(file)
            src = os.path.join(root, file)
            dst01 = Path(src).with_suffix('').with_suffix('')
            dst11 = Path(dst01).with_suffix('.workcsv')
            #print("dst", dst11)
            shutil.copyfile(src, dst11)

for root, dirs, files in os.walk(local):
    for file in files:
        if file.endswith('.workcsv'):
            df = pd.read_csv(os.path.join(root, file), dtype=str, encoding='utf8', delimiter=';')
#задаем новый датафрейм, его столбы
            my_df1 = pd.DataFrame(columns = ['telco_id','begin_connection_time','duration','call_type_id','supplement_service_id','in_abonent_type','out_abonent_type','switch_id','inbound_bunch_type','inbound_bunch_gsm','inbound_bunch_mac','inbound_bunch_vpi','inbound_bunch_vci','outbound_bunch_type','outbound_bunch_gsm','outbound_bunch_mac','outbound_bunch_vpi','outbound_bunch_vci','term_cause','phone_card_number','dialed_digits','forwarding_identifier','border_switch_id','roaming_partner_id','message','in_identifier_type','in_pstn_directory_number','in_pstn_internal_number','in_gsm_directory_number','in_gsm_imsi','in_gsm_imei','in_gsm_icc','in_cdma_directory_number','in_cdma_imsi','in_cdma_icc','in_cdma_esn','in_cdma_min','in_voip_ip_address','in_voip_originator_name','in_voip_calling_number','in_begin_location_type','in_begin_location_mobile_lac','in_begin_location_mobile_cell','in_begin_location_mobile_ta','in_begin_location_wireless_cell','in_begin_location_wireless_mac','in_begin_location_geolocation_latitude_grade','in_begin_location_geolocation_longitude_grade','in_begin_location_geolocation_projection_type','in_begin_location_iplocation_ip_address','in_begin_location_iplocation_ip_port','in_end_location_type','in_end_location_mobile_lac','in_end_location_mobile_cell','in_end_location_mobile_ta','in_end_location_wireless_cell','in_end_location_wireless_mac','in_end_location_geolocation_latitude_grade','in_end_location_geolocation_longitude_grade','in_end_location_geolocation_projection_type','in_end_location_iplocation_ip_address','in_end_location_iplocation_ip_port','out_identifier_type','out_pstn_directory_number','out_pstn_internal_number','out_gsm_directory_number','out_gsm_imsi','out_gsm_imei','out_gsm_icc','out_cdma_directory_number','out_cdma_imsi','out_cdma_icc','out_cdma_esn','out_cdma_min','out_voip_ip_address','out_voip_originator_name','out_voip_calling_number','out_begin_location_type','out_begin_location_mobile_lac','out_begin_location_mobile_cell','out_begin_location_mobile_ta','out_begin_location_wireless_cell','out_begin_location_wireless_mac','out_begin_location_geolocation_latitude_grade','out_begin_location_geolocation_longitude_grade','out_begin_location_geolocation_projection_type','out_begin_location_iplocation_ip_address','out_begin_location_iplocation_ip_port','out_end_location_type','out_end_location_mobile_lac','out_end_location_mobile_cell','out_end_location_mobile_ta','out_end_location_wireless_cell','out_end_location_wireless_mac','out_end_location_geolocation_latitude_grade','out_end_location_geolocation_longitude_grade','out_end_location_geolocation_projection_type','out_end_location_iplocation_ip_address','out_end_location_iplocation_ip_port','data_content_id','ss7_opc','ss7_dpc'])
#сохраняем новый датафрейм в переменную
            df1 = pd.DataFrame(my_df1)
#получения количества строк исходного файла и вывод на печать
            string_count = df.shape[0]
# получение количества столбцов исходного файла  и вывод на печать, можно ещё делать через df1.columns.size
            column_count = df.shape[1]
#получения количества строк выходного файла  и вывод на печать
            string_count1 = df1.shape[0]
# получение количества столбцов выходного файла и вывод на печать, можно ещё делать через df1.columns.size
            column_count1 = df1.shape[1]

#делаем матрицу заполненную нулями нужной размерности
            #data = np.zeros((string_count, column_count1), dtype=np.int8)
#делаем матрицу заполненную пустотами нужной размерности
            data = np.full((string_count, column_count1), np.nan)
#задаем датафрейм нужной размерности
            my_df1 = pd.DataFrame(data, columns = ['telco_id','begin_connection_time','duration','call_type_id','supplement_service_id','in_abonent_type','out_abonent_type','switch_id','inbound_bunch_type','inbound_bunch_gsm','inbound_bunch_mac','inbound_bunch_vpi','inbound_bunch_vci','outbound_bunch_type','outbound_bunch_gsm','outbound_bunch_mac','outbound_bunch_vpi','outbound_bunch_vci','term_cause','phone_card_number','dialed_digits','forwarding_identifier','border_switch_id','roaming_partner_id','message','in_identifier_type','in_pstn_directory_number','in_pstn_internal_number','in_gsm_directory_number','in_gsm_imsi','in_gsm_imei','in_gsm_icc','in_cdma_directory_number','in_cdma_imsi','in_cdma_icc','in_cdma_esn','in_cdma_min','in_voip_ip_address','in_voip_originator_name','in_voip_calling_number','in_begin_location_type','in_begin_location_mobile_lac','in_begin_location_mobile_cell','in_begin_location_mobile_ta','in_begin_location_wireless_cell','in_begin_location_wireless_mac','in_begin_location_geolocation_latitude_grade','in_begin_location_geolocation_longitude_grade','in_begin_location_geolocation_projection_type','in_begin_location_iplocation_ip_address','in_begin_location_iplocation_ip_port','in_end_location_type','in_end_location_mobile_lac','in_end_location_mobile_cell','in_end_location_mobile_ta','in_end_location_wireless_cell','in_end_location_wireless_mac','in_end_location_geolocation_latitude_grade','in_end_location_geolocation_longitude_grade','in_end_location_geolocation_projection_type','in_end_location_iplocation_ip_address','in_end_location_iplocation_ip_port','out_identifier_type','out_pstn_directory_number','out_pstn_internal_number','out_gsm_directory_number','out_gsm_imsi','out_gsm_imei','out_gsm_icc','out_cdma_directory_number','out_cdma_imsi','out_cdma_icc','out_cdma_esn','out_cdma_min','out_voip_ip_address','out_voip_originator_name','out_voip_calling_number','out_begin_location_type','out_begin_location_mobile_lac','out_begin_location_mobile_cell','out_begin_location_mobile_ta','out_begin_location_wireless_cell','out_begin_location_wireless_mac','out_begin_location_geolocation_latitude_grade','out_begin_location_geolocation_longitude_grade','out_begin_location_geolocation_projection_type','out_begin_location_iplocation_ip_address','out_begin_location_iplocation_ip_port','out_end_location_type','out_end_location_mobile_lac','out_end_location_mobile_cell','out_end_location_mobile_ta','out_end_location_wireless_cell','out_end_location_wireless_mac','out_end_location_geolocation_latitude_grade','out_end_location_geolocation_longitude_grade','out_end_location_geolocation_projection_type','out_end_location_iplocation_ip_address','out_end_location_iplocation_ip_port','data_content_id','ss7_opc','ss7_dpc'])
#сохраняем в переменную
            df1 = pd.DataFrame(my_df1)
#print(df1)

#в новый датафрейм занесем значения из исходного файла, из 'Connect time (orig.)' в 'begin_connection_time'

#зададим столбцу тип данных

            #print("для первого случая")
            #print(df.loc[34, 'Connect time (orig.)'])
            #print(type(df.loc[34, 'Connect time (orig.)']))
            #df['Connect time (orig.)'] = pd.to_datetime(df['Connect time (orig.)'])
            #print(df.loc[34, 'Connect time (orig.)'])
            #print(type(df.loc[34, 'Connect time (orig.)']))

            #print("для второго случая")

            #print(df1.loc[11, 'begin_connection_time'])
            #print(type(df1.loc[11, 'begin_connection_time']))
            #df1['begin_connection_time'] = pd.to_datetime(df1['begin_connection_time'])
            #print(df1.loc[11, 'begin_connection_time'])
            #print(type(df1.loc[11, 'begin_connection_time']))

            #print("присвоение типов окончено")

#заполняем дефолтными значениями, нулями
            df1['telco_id'] = df1['call_type_id'] = df1['supplement_service_id'] = df1['in_abonent_type'] = df1['out_abonent_type'] = df1['switch_id'] = df1['inbound_bunch_type'] = df1['outbound_bunch_type'] = df1['border_switch_id'] = df1['in_begin_location_type'] = df1['in_begin_location_mobile_lac'] = df1['in_begin_location_mobile_cell'] = df1['in_begin_location_mobile_ta'] = df1['in_end_location_type'] = df1['in_end_location_mobile_lac'] = df1['in_end_location_mobile_cell'] = int(0)
# заполняем дефолтными значениями, двойками
            df1['in_identifier_type'] = df1['in_identifier_type'] = int(2)
# заполняем  значениями соответствий столбцов
#print(df1['duration'])
            df['Connect time (orig.)'] = pd.to_datetime(df['Connect time (orig.)'])
            df1['begin_connection_time'] = pd.to_datetime(df1['begin_connection_time'])
            df1['begin_connection_time'] = pd.to_datetime(df['Connect time (orig.)'])
            df1['duration'] = df['In-call time (orig., sec)']
            df1['inbound_bunch_gsm'] = df['Origination endpoint (ID)']
            df1['outbound_bunch_gsm'] = df['Termination endpoint (ID)']
            df1['dialed_digits'] = df['SRC Number In']
            df1['in_gsm_directory_number'] = df['DST number used for routing']
            df1['in_gsm_imsi'] = df['CDR ID']
            df1['out_gsm_directory_number'] = df['SRC Number Out']
            df1['out_gsm_imsi'] = df['CDR ID']
# заполняем значениями после операций с маской
            df['Disconnect code (orig.) (ID)'] = df['Disconnect code (orig.) (ID)'].astype(str)
            df1['term_cause'] = df1['term_cause'].astype(str)
            #print(type(df.loc[11, 'Disconnect code (orig.) (ID)']))
            #
            df1['term_cause'] = df['Disconnect code (orig.) (ID)'].apply(lambda x = "^$": x if re.match(r'Q.850 | ', x) else "")
            df1['term_cause'] = df1['term_cause'].apply(lambda x : x.replace('Q.850 | ',''))
            df1['term_cause'] = df1['term_cause'].apply(lambda x: re.compile(r'\D').sub('', x))

            #SRC IP address
            df['SRC IP address'] = df['SRC IP address'].astype(str)
            data = np.full((string_count, 1), np.nan)
            my_df2 = pd.DataFrame(data, columns=['SRC IP address_IP11'])
            a = pd.DataFrame(my_df2)
            a['SRC IP address_IP11'] = df['SRC IP address'].copy()
            a['SRC IP address_PORT'] = a['SRC IP address_IP11'].str.extract(r'(:\w+.)')
            aIP1 = a['SRC IP address_PORT'].copy()
            aIP2 = aIP1.apply(lambda x: x.replace(':', '')).copy()
            df1['in_begin_location_iplocation_ip_port'] = aIP2.copy()
            #print("in_begin_location_iplocation_ip_port", df1['in_begin_location_iplocation_ip_port'])

            aIP3 = a['SRC IP address_IP11'].str.extract(r'(\w+.\w+.\w+.\w+.)').copy()
            aIP4 = aIP3[0].apply(lambda x: x.replace(":", "")).copy()
            df1['in_begin_location_iplocation_ip_address'] = aIP4.copy()
            #print("in_begin_location_iplocation_ip_address", df1['in_begin_location_iplocation_ip_address'])




            #DST IP address
            df['DST IP address'] = df['DST IP address'].astype(str)
            data = np.full((string_count, 1), np.nan)
            my_df3 = pd.DataFrame(data, columns=['DST IP address11'])
            b = pd.DataFrame(my_df3)
            b['DST IP address11'] = df['DST IP address'].copy()
            b['DST IP address_PORT'] = b['DST IP address11'].str.extract(r'(:\w+.)')
            bIP1 = b['DST IP address_PORT'].copy()
            bIP2 = bIP1.apply(lambda x: x.replace(':', '')).copy()
            print("bIP2", bIP2)
            df1['in_end_location_iplocation_ip_port'] = bIP2.copy()
            print("in_end_location_iplocation_ip_port", df1['in_end_location_iplocation_ip_port'])

            bIP3 = b['DST IP address11'].str.extract(r'(\w+.\w+.\w+.\w+.)').copy()
            bIP4 = bIP3[0].apply(lambda x: x.replace(":", "")).copy()
            df1['in_end_location_iplocation_ip_address'] = bIP4.copy()
            #print("in_end_location_iplocation_ip_address", df1['in_end_location_iplocation_ip_address'])


# сохраняем новый датафрейм в csv
            df1.to_csv(os.path.join(root, file), sep = ';', encoding='utf8', index = True)
            src = os.path.join(root, file)
            dst01 = Path(src).with_suffix('')
            dst11 = Path(dst01).with_suffix('.csv')
            #print("dst", dst11)
            shutil.copyfile(src, dst11)
            os.remove(os.path.join(local, file))


elapsed = datetime.now() - start
print(f"Затраченное время: {elapsed.total_seconds()} сек")