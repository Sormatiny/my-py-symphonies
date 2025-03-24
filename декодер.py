import os
from pathlib import Path
import shutil
import pandas as pd
import numpy as np
import re
import ast


#задаем пути
local = '/home/atis/Документы/Task5/1'
remote = '/home/atis/Документы/Task5/2'

import base64

x = 'nan'
padding_len = len(x) % 4
x += padding_len * '='
base64_bytes = x.encode(encoding="utf-8")
message_bytes = base64.b64decode(base64_bytes)
y = message_bytes.decode('utf-8')
print (y)




#padding_len = len(x) % 4
#x += padding_len * '='
#base64_bytes = x.encode(encoding="ascii")
#message_bytes = base64.b64decode(base64_bytes)
#y = message_bytes.decode('ascii')

#print(base64.b64encode('\x9d\xa9'))