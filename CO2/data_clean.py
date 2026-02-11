import pandas as pd
data = pd.read_csv('GCB2022v27_MtCO2_flat.csv')
data = data.fillna(0)

with open('dataset.csv', 'w+', encoding='utf-8') as f:
    for line in data.values:
        if line[3] == 0.0:
            continue
        if line[0] == "Global" or line[0] == "International Transport":
            continue
        f.write(str(line[0])+'\t'+str(line[2])+'\t'
                +str(line[3])+'\t'+str(line[4])+'\t'+str(line[5])+'\t'+
                str(line[6])+'\t'+str(line[7])+'\t'+str(line[8])+'\t'+
                str(line[9])+'\t'+str(line[10])+'\n')
