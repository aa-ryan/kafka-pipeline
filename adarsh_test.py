import random
import pandas as pd

def check_severity(temperature, heart_rate, respiration_rate, blood_oxygen, blood_pressure):

    
    def check_temperature(temperature):
        if 0 <= temperature <= 35 or temperature > 37.5:
            return 2
        if 35.1 <= temperature <= 36.5:
            return 1
        if 36.6 <= temperature <= 37.5:
            return 0

            
    def check_heart_rate(heart_rate):
        if 0 <= heart_rate <= 40:
            return 2
        if 40 <= heart_rate <= 50:
            return 1
        if 51 <= heart_rate <= 100:
            return 0
        if 101 <= heart_rate <= 110:
            return 1
        if 111 <= heart_rate <= 130:
            return 2
        if heart_rate > 130:
            return 3
        
        
    def check_respiration_rate(respiration_rate):
        if 0 <= respiration_rate <= 8:
            return 2
        if 9 <= respiration_rate <= 14:
            return 0
        if 15 <= respiration_rate <= 20:
            return 1
        if 21 <= respiration_rate <= 30:
            return 2
        if respiration_rate > 30:
            return 3
        
        
    def check_blood_oxygen(blood_oxygen):
        if 0 <= blood_oxygen <= 90:
            return 3
        if 91 <= blood_oxygen <= 93:
            return 2
        if 94 <= blood_oxygen <= 95:
            return 1
        if blood_oxygen > 95:
            return 0
        
        
    def check_blood_pressure(blood_pressure):
        if 0 <= blood_pressure < 70:
            return 3
        if 70 <= blood_pressure <= 80:
            return 2
        if 81 <= blood_pressure <= 100:
            return 1
        if 101 <= blood_pressure <= 180:
            return 0
        if 180 <= blood_pressure <= 200:
            return 1
        if blood_pressure > 200:
            return 2


if __name__ == "__main__":
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
            dict_details['RespirationRate'] = str(random.randint(lower_limit_resp, upper_limit_resp))
            dict_details['OxygenSaturation'] = str(random.randint(lower_limit_oxy, upper_limit_oxy))
            dict_details['BloodPressure'] = str(random.randint(lower_limit_bp, upper_limit_bp))

            df_final = df_final.append(dict_details, ignore_index=True)

    df_final.to_csv('Dataset.csv', index=False)

    print(df_final)

