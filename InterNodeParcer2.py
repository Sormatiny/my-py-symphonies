from datetime import datetime
start = datetime.now()

import os
from pathlib import Path
import shutil
import pandas as pd
import numpy as np
import re
import base64
import ast

#локальная папка
local = '/home/atis/Документы/Task5/1'
#удаленная папка
remote = '/home/atis/Документы/Task5/2'

def decod(x):
    if x != 'nan':
        padding_len = len(x) % 4
        x += padding_len * '='
        base64_bytes = x.encode(encoding="utf-8")
        message_bytes = base64.b64decode(base64_bytes)
        y = message_bytes.decode('utf-8')
        return y

#создаем открываем лог файл
pathoflogfile = os.path.join(local, 'log.txt')
#поменять на А
log_file = open(pathoflogfile, "w")
#прочли файл, теперь закрываем
log_file.close()

# рекурсивно проходим все подпапки в локальной директории
for root, dirs, files in os.walk(local):
    for dir in dirs:
        #print("dir:", dir)
        subdir = str(root + '/' + dir)
        #print("subdir", subdir)
        subdir1 = str(' subdir+ ')
        #print("subdir1", subdir1)

        files1 = Path(subdir1).glob("*")
        #print("поддиректория локальной директории:" , dir)
        #print("абсолютный путь нужной директории", os.path.abspath(subdir))

        log_file = open(pathoflogfile, "r")
        content = open(pathoflogfile, "r").read()
        log_file.close()
        if re.search(subdir, content):
            print('Строка найдена!')
        else:
            #print('Строка не найдена!')
            log_file = open(pathoflogfile, "a")
            log_file.write(subdir + '\r')
            log_file.close()
            for root1, dirs1, files1 in os.walk(subdir):
                for file1 in files1:
                    #print(root1)
                    #print(file1)
                    if file1.endswith('.csv'):
                        src = os.path.join(root1, file1)
                        src2 = os.path.join(root1, file1 + '.old')
                        os.rename(src, src2)
                        filepathdst = src2.replace('passive86', 'cdr')
                        filenamedst = os.path.basename(filepathdst)
                        shutil.copyfile(os.path.join(root1, file1 + '.old'), filepathdst)
                        os.rename(filepathdst, os.path.join(remote, filenamedst))
                        # os.rename(src, Path(src).with_suffix('.old'))

for root, dirs, files in os.walk(remote):
    for file in files:
        if file.endswith('.old'):
            df = pd.read_csv(os.path.join(root, file), dtype=str, encoding='utf8', delimiter=',')
            # задаем новый датафрейм, его столбы
            my_df1 = pd.DataFrame(columns=['telco_id', 'begin_connection_time', 'duration', 'call_type_id', 'supplement_service_id',
                         'in_abonent_type', 'out_abonent_type', 'switch_id', 'inbound_bunch_type', 'inbound_bunch_gsm',
                         'inbound_bunch_mac', 'inbound_bunch_vpi', 'inbound_bunch_vci', 'outbound_bunch_type',
                         'outbound_bunch_gsm', 'outbound_bunch_mac', 'outbound_bunch_vpi', 'outbound_bunch_vci',
                         'term_cause', 'phone_card_number', 'dialed_digits', 'forwarding_identifier',
                         'border_switch_id', 'roaming_partner_id', 'message', 'in_identifier_type',
                         'in_pstn_directory_number', 'in_pstn_internal_number', 'in_gsm_directory_number',
                         'in_gsm_imsi', 'in_gsm_imei', 'in_gsm_icc', 'in_cdma_directory_number', 'in_cdma_imsi',
                         'in_cdma_icc', 'in_cdma_esn', 'in_cdma_min', 'in_voip_ip_address', 'in_voip_originator_name',
                         'in_voip_calling_number', 'in_begin_location_type', 'in_begin_location_mobile_lac',
                         'in_begin_location_mobile_cell', 'in_begin_location_mobile_ta',
                         'in_begin_location_wireless_cell', 'in_begin_location_wireless_mac',
                         'in_begin_location_geolocation_latitude_grade',
                         'in_begin_location_geolocation_longitude_grade',
                         'in_begin_location_geolocation_projection_type', 'in_begin_location_iplocation_ip_address',
                         'in_begin_location_iplocation_ip_port', 'in_end_location_type', 'in_end_location_mobile_lac',
                         'in_end_location_mobile_cell', 'in_end_location_mobile_ta', 'in_end_location_wireless_cell',
                         'in_end_location_wireless_mac', 'in_end_location_geolocation_latitude_grade',
                         'in_end_location_geolocation_longitude_grade', 'in_end_location_geolocation_projection_type',
                         'in_end_location_iplocation_ip_address', 'in_end_location_iplocation_ip_port',
                         'out_identifier_type', 'out_pstn_directory_number', 'out_pstn_internal_number',
                         'out_gsm_directory_number', 'out_gsm_imsi', 'out_gsm_imei', 'out_gsm_icc',
                         'out_cdma_directory_number', 'out_cdma_imsi', 'out_cdma_icc', 'out_cdma_esn', 'out_cdma_min',
                         'out_voip_ip_address', 'out_voip_originator_name', 'out_voip_calling_number',
                         'out_begin_location_type', 'out_begin_location_mobile_lac', 'out_begin_location_mobile_cell',
                         'out_begin_location_mobile_ta', 'out_begin_location_wireless_cell',
                         'out_begin_location_wireless_mac', 'out_begin_location_geolocation_latitude_grade',
                         'out_begin_location_geolocation_longitude_grade',
                         'out_begin_location_geolocation_projection_type', 'out_begin_location_iplocation_ip_address',
                         'out_begin_location_iplocation_ip_port', 'out_end_location_type',
                         'out_end_location_mobile_lac', 'out_end_location_mobile_cell', 'out_end_location_mobile_ta',
                         'out_end_location_wireless_cell', 'out_end_location_wireless_mac',
                         'out_end_location_geolocation_latitude_grade', 'out_end_location_geolocation_longitude_grade',
                         'out_end_location_geolocation_projection_type', 'out_end_location_iplocation_ip_address',
                         'out_end_location_iplocation_ip_port', 'data_content_id', 'ss7_opc', 'ss7_dpc'])
            df1 = pd.DataFrame(my_df1)
            string_count = df.shape[0]
            column_count = df.shape[1]
            string_count1 = df1.shape[0]
            column_count1 = df1.shape[1]
            data = np.full((string_count, column_count1), np.nan)
            my_df1 = pd.DataFrame(data, columns=['telco_id', 'begin_connection_time', 'duration', 'call_type_id',
                                                 'supplement_service_id', 'in_abonent_type', 'out_abonent_type',
                                                 'switch_id', 'inbound_bunch_type', 'inbound_bunch_gsm',
                                                 'inbound_bunch_mac', 'inbound_bunch_vpi', 'inbound_bunch_vci',
                                                 'outbound_bunch_type', 'outbound_bunch_gsm', 'outbound_bunch_mac',
                                                 'outbound_bunch_vpi', 'outbound_bunch_vci', 'term_cause',
                                                 'phone_card_number', 'dialed_digits', 'forwarding_identifier',
                                                 'border_switch_id', 'roaming_partner_id', 'message',
                                                 'in_identifier_type', 'in_pstn_directory_number',
                                                 'in_pstn_internal_number', 'in_gsm_directory_number', 'in_gsm_imsi',
                                                 'in_gsm_imei', 'in_gsm_icc', 'in_cdma_directory_number',
                                                 'in_cdma_imsi', 'in_cdma_icc', 'in_cdma_esn', 'in_cdma_min',
                                                 'in_voip_ip_address', 'in_voip_originator_name',
                                                 'in_voip_calling_number', 'in_begin_location_type',
                                                 'in_begin_location_mobile_lac', 'in_begin_location_mobile_cell',
                                                 'in_begin_location_mobile_ta', 'in_begin_location_wireless_cell',
                                                 'in_begin_location_wireless_mac',
                                                 'in_begin_location_geolocation_latitude_grade',
                                                 'in_begin_location_geolocation_longitude_grade',
                                                 'in_begin_location_geolocation_projection_type',
                                                 'in_begin_location_iplocation_ip_address',
                                                 'in_begin_location_iplocation_ip_port', 'in_end_location_type',
                                                 'in_end_location_mobile_lac', 'in_end_location_mobile_cell',
                                                 'in_end_location_mobile_ta', 'in_end_location_wireless_cell',
                                                 'in_end_location_wireless_mac',
                                                 'in_end_location_geolocation_latitude_grade',
                                                 'in_end_location_geolocation_longitude_grade',
                                                 'in_end_location_geolocation_projection_type',
                                                 'in_end_location_iplocation_ip_address',
                                                 'in_end_location_iplocation_ip_port', 'out_identifier_type',
                                                 'out_pstn_directory_number', 'out_pstn_internal_number',
                                                 'out_gsm_directory_number', 'out_gsm_imsi', 'out_gsm_imei',
                                                 'out_gsm_icc', 'out_cdma_directory_number', 'out_cdma_imsi',
                                                 'out_cdma_icc', 'out_cdma_esn', 'out_cdma_min', 'out_voip_ip_address',
                                                 'out_voip_originator_name', 'out_voip_calling_number',
                                                 'out_begin_location_type', 'out_begin_location_mobile_lac',
                                                 'out_begin_location_mobile_cell', 'out_begin_location_mobile_ta',
                                                 'out_begin_location_wireless_cell', 'out_begin_location_wireless_mac',
                                                 'out_begin_location_geolocation_latitude_grade',
                                                 'out_begin_location_geolocation_longitude_grade',
                                                 'out_begin_location_geolocation_projection_type',
                                                 'out_begin_location_iplocation_ip_address',
                                                 'out_begin_location_iplocation_ip_port', 'out_end_location_type',
                                                 'out_end_location_mobile_lac', 'out_end_location_mobile_cell',
                                                 'out_end_location_mobile_ta', 'out_end_location_wireless_cell',
                                                 'out_end_location_wireless_mac',
                                                 'out_end_location_geolocation_latitude_grade',
                                                 'out_end_location_geolocation_longitude_grade',
                                                 'out_end_location_geolocation_projection_type',
                                                 'out_end_location_iplocation_ip_address',
                                                 'out_end_location_iplocation_ip_port', 'data_content_id', 'ss7_opc',
                                                 'ss7_dpc'])
            df1 = pd.DataFrame(my_df1)

            #print("исходный",df)
            #print("что получилось в итоге", df1)
            #print(df['type'])

            dfequaltypeone = df.copy()
            dfequaltypeone = dfequaltypeone[dfequaltypeone['type'].str.contains('1|2')].copy()

            df1['telco_id'] = df1['inbound_bunch_type'] = df1['outbound_bunch_type'] = df1['border_switch_id'] = int(0)
            df1['switch_id'] = int(1)
            df1['duration'] = dfequaltypeone['duration']
            df1['out_gsm_imsi'] = dfequaltypeone['from_imsi']
            df1['out_gsm_imei'] = dfequaltypeone['from_imei']
            dfequaltypeone['start'] = dfequaltypeone['start'].astype(int)
            df1['begin_connection_time'] = dfequaltypeone['start'].apply(lambda x: datetime.fromtimestamp(x))

            dfequaltypeone['message'] = dfequaltypeone['message']
            dfequaltypeone['message'] = dfequaltypeone['message'].apply(lambda x: str(x))
            dfequaltypeone['message'] = dfequaltypeone['message'].apply(lambda x: decod(x))
            df1['message'] = dfequaltypeone['message']
            #print(df['message'].notnull())

            dfequaltypeone['redirections'] = dfequaltypeone['redirections'].astype(str)
            dfequaltypeone['redirections'] = dfequaltypeone['redirections'].apply(lambda x: x[:4] if x != 'nan' else None)


            df1['supplement_service_id'] = int(0)



            df1['in_identifier_type'] = dfequaltypeone['phases'].apply(lambda x: int(1) if str(x[:4]) == str(3900) else int(2) if str(x[:4]) == str(3700) else None).astype('Int8')

            dfequaltypeone['to_imsi'] = dfequaltypeone['to_imsi'].apply(lambda x: str(x))
            #print(dfequaltypeone['to_imsi'])
            dfred1 = pd.DataFrame()
            dfred1['to_imsi'] = dfequaltypeone['to_imsi'].copy().astype(str)
            #print(dfred1)
            dfred2 = pd.DataFrame()
            dfred2['in_identifier_type'] = df1['in_identifier_type'].copy().astype(str)
            #print(dfred2)
            dfred3 = pd.merge(dfred1, dfred2, left_index=True, right_index=True)
            #print(dfred3)
            dfred3['result'] = 'nan'
            #print(dfred3['in_identifier_type'])

            for i, j in dfred3.iterrows():
                if dfred3.iloc[i, 1] == '2':
                    dfred3.iloc[i, 2] = dfred3.iloc[i, 0]
                    #print(dfred3.iloc[i, 2])

            df1['in_gsm_imsi'] = dfred3['result'].copy()
            df1['in_gsm_imsi'] = df1['in_gsm_imsi'].apply(lambda x: x.replace('nan', ''))
            del dfred1, dfred2, dfred3


            dfequaltypeone['to_imei'] = dfequaltypeone['to_imei'].apply(lambda x: str(x))
            #print(dfequaltypeone['to_imsi'])
            dfred1 = pd.DataFrame()
            dfred1['to_imei'] = dfequaltypeone['to_imei'].copy().astype(str)
            #print(dfred1)
            dfred2 = pd.DataFrame()
            dfred2['in_identifier_type'] = df1['in_identifier_type'].copy().astype(str)
            #print(dfred2)
            dfred3 = pd.merge(dfred1, dfred2, left_index=True, right_index=True)
            #print(dfred3)
            dfred3['result'] = 'nan'
            #print(dfred3['in_identifier_type'])

            for i, j in dfred3.iterrows():
                if dfred3.iloc[i, 1] == '2':
                    dfred3.iloc[i, 2] = dfred3.iloc[i, 0]
                    #print(dfred3.iloc[i, 2])

            df1['in_gsm_imei'] = dfred3['result'].copy()
            df1['in_gsm_imei'] = df1['in_gsm_imei'].apply(lambda x: x.replace('nan', ''))
            del dfred1, dfred2, dfred3

            dfred1 = pd.DataFrame()
            dfred1 = df['redirections'].copy().astype(str)
            #print(dfred1)
            df1['call_type_id'] = dfred1.apply(lambda x: int(1) if x == 'nan' else None)
            #print(df1['call_type_id'])
            del dfred1




            #сохраняем файлы, чистим хвосты
            df1.to_csv(os.path.join(root, file), sep=';', encoding='utf8', index=True)
            src = os.path.join(root, file)
            dst01 = Path(src).with_suffix('').with_suffix('')
            dst11 = Path(dst01).with_suffix('.csv')
            shutil.copyfile(src, dst11)
            os.remove(os.path.join(root, file))



elapsed = datetime.now() - start
print(f"Затраченное время: {elapsed.total_seconds()} сек")