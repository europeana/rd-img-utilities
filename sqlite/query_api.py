import requests
import json
import pandas as pd

data = {
    'id_1':'/00738/plink__f_5_226086',
    'id_2':'/00738/plink__f_5_226084'
}

response = requests.get('http://127.0.0.1:6008/size')
print(json.loads(response.content))

response = requests.post('http://127.0.0.1:6008/list',data=data)
#print(json.loads(response.content))
df = pd.DataFrame.from_dict(json.loads(response.content))
print(df)



