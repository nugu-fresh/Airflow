import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
print(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
print(type(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))


for id in range(1, 6):
     file_name = 'Input{}_scaler.pkl'.format(id)
     file_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__))) + "/iscalers/" + file_name
     print(file_path)