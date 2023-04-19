# <YOUR_IMPORTS>
import os
import dill
import json
import pandas as pd
from pandas import json_normalize

path = os.environ.get('PROJECT_PATH', '.')
#path = 'C:/Users/ACER/airflow_hw'

model = object

def predict():
    modelFileName = os.listdir(f"{path}/data/models")[0]
    predictDict = {}

    with open(f'{path}/data/models/{modelFileName}', 'rb') as file:
        model = dill.load(file)

    testFileList = os.listdir(f"{path}/data/test")

    for testFileName in testFileList:
        with open(f"{path}/data/test/{testFileName}", 'rb') as testFile:
            dict_json = json.load(testFile)
            df = json_normalize(dict_json)
            predictDict[testFileName.replace('.json', '')] = model.predict(df)

    predictResults = pd.DataFrame(predictDict.items(), columns=['fileName', 'predict'])

    predictResults.to_csv(f'{path}/data/predictions/result.csv')

if __name__ == '__main__':
    predict()
