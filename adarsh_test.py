import pandas as pd
import random

df_id = pd.DataFrame()
dict_id = {}
dict_details = {}

df_final = pd.DataFrame()

# ---- Patient ID----------------------------------------------
lower_limit = 1000
upper_limit = 9999

patient_id = random.sample(range(lower_limit, upper_limit), 30)
for i in range(len(patient_id)):
    dict_id['PatientId'] = patient_id[i]

    df_id = df_id.append(dict_id, ignore_index=True)
# ------Temperature---------------------------------------------

lower_limit_temp = 36.60
upper_limit_temp = 37.50

lower_limit_heart = 51
upper_limit_heart = 100

lower_limit_resp = 9
upper_limit_resp = 14

lower_limit_oxy = 96
upper_limit_oxy = 100

lower_limit_bp = 101
upper_limit_bp = 180

for i in range(0, 30):

    for j in range(len(df_id['PatientId'])):
        dict_details['PatientId'] = str(df_id['PatientId'][j])
        dict_details['Temperature'] = str(round(random.uniform(lower_limit_temp, upper_limit_temp), 2))
        dict_details['HeartRate'] = str(random.randint(lower_limit_heart, upper_limit_heart))
        dict_details['RespirationRatio'] = str(random.randint(lower_limit_resp, upper_limit_resp))
        dict_details['OxygenSaturation'] = str(random.randint(lower_limit_oxy, upper_limit_oxy))
        dict_details['BloodPressure'] = str(random.randint(lower_limit_bp, upper_limit_bp))

        df_final = df_final.append(dict_details, ignore_index=True)

df_final.to_csv('/Users/vineetsingh/PycharmProjects/client_coverage/final.csv', index=False)

print(df_final)

