import os
import subprocess
from datetime import datetime
from ast import literal_eval as eval
import pandas as pd

date = datetime.now().strftime("%Y-%m-%d")
DIR = "/home/ETL/csv/"
DIR_JSON = "/home/ETL/json/"
j = 0

try:
    for file in os.listdir(DIR):
        if file.endswith(".csv"):
            #print(DIR+file)
            dfpj = pd.read_csv(os.path.join(DIR, file))
            dataset = {}
            cards = {'card{0}'.format(i):eval(dfpj['card_data'].values[i]) for i in range(len(dfpj.index))}
            for card in cards: 
                dataset[card] = {'x{0}'.format(i):cards[card][i][0] for i in range(len(cards[card]))}
                dataset[card].update({'y{0}'.format(i):cards[card][i][1] for i in range(len(cards[card]))})
            dfcard = pd.DataFrame.from_dict(dataset, orient = 'index').reset_index()
            df = pd.concat([dfpj,dfcard], axis=1).drop(['card_data', 'index'], axis=1)
            if file.startswith('type_0'):
                if file[10] == '.':
                    # print(file[8:10])
                    df.to_json(DIR_JSON + 'type_0' + file[8:10] + '.json', orient = 'records')
                else:
                    # print(file[8:11])
                    df.to_json(DIR_JSON + 'type_0' + file[8:11] + '.json', orient = 'records')
            elif file.startswith('pump'):
                if file[7] == '.':
                    # print(file[5:7])
                    df.to_json(DIR_JSON + 'type_1' + file[5:7] + '.json', orient = 'records')
                else:
                    # print(file[5:8])
                    df.to_json(DIR_JSON + 'type_1' + file[5:8] + '.json', orient = 'records')
        print("{}: Data transformation done:{}, file: {}".format(datetime.now().strftime("%a %h %d %H:%M:%S %Y"), j, DIR+file))
        j += 1
    print("{}: csv to json transformation completed".format(datetime.now().strftime("%a %h %d %H:%M:%S %Y")))
except Exception as ex:
    print("{}: failed to transform csv to json: {}".format(datetime.now().strftime("%a %h %d %H:%M:%S %Y"), ex))

try:        
    for file in os.listdir(DIR_JSON): 
        if file.endswith(".json"):
            try:
                p = subprocess.Popen(['mongoimport', '-d', 'cards','-c', 
                                  '{}'.format('surface_cards' if file.startswith('type_0') else 'type_1_cards'),
                                  '--authenticationDatabase', 'admin', '-u', 'admin', 
                                  '-p', 'UnfixedFruit','--file', DIR_JSON+file, '--jsonArray'],
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in p.stdout.readlines():
                    print(line)
                p.stdout.close()
                return_code = p.wait()
                if return_code:
                    raise subprocess.CalledProcessError(file, return_code)
            except Exception as ex:
                print("{}: MONGODB IMPORT FAILED: {} on file {}".format(datetime.now().strftime("%a %h %d %H:%M:%S %Y"),ex,file))
    print("{}: MONGODB IMPORT SUCCESSFULL".format(datetime.now().strftime("%a %h %d %H:%M:%S %Y")))
except Exception as ex:
    print("{}: MONGODB FAILED: {}".format(datetime.now().strftime("%a %h %d %H:%M:%S %Y"),ex))
