# Produce Json from Dictionary
import random
import json
from confluent_kafka import Producer
import time, json

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf



# ---- Patient ID----------------------------------------------
lower_limit = 1000
upper_limit = 9999

# patient_id = random.sample(range(lower_limit, upper_limit), 30)
# ward = ['ward_a', 'ward_b', 'ward_c']
# for i in range(len(patient_id)):
#     dict_id['PatientId'] = patient_id[i]

# ------Temperature---------------------------------------------

lower_limit_temp = 37.00
upper_limit_temp = 37.50

lower_limit_heart = 51
upper_limit_heart = 90

lower_limit_resp = 12
upper_limit_resp = 18

lower_limit_oxy = 92
upper_limit_oxy = 100

lower_limit_bp = 111
upper_limit_bp = 180

# for i in range(0,100):

#     for j in range(len(df_id['PatientId'])):
#         dict_details['PatientId'] = str(df_id['PatientId'][j])
#         dict_details['ward'] = str(df_id['ward_number'][j])
#         dict_details['Temperature'] = str(round(random.uniform(lower_limit_temp, upper_limit_temp), 2))
#         dict_details['HeartRate'] = str(random.randint(lower_limit_heart, upper_limit_heart))
#         dict_details['RespirationRatio'] = str(random.randint(lower_limit_resp, upper_limit_resp))
#         dict_details['OxygenSaturation'] = str(random.randint(lower_limit_oxy, upper_limit_oxy))
#         dict_details['BloodPressure'] = str(random.randint(lower_limit_bp, upper_limit_bp))

#         df_final = df_final.append(dict_details, ignore_index=True)
        
        
        
def generate_patientID():
    patient_id = random.sample(range(lower_limit, upper_limit), 30)
    return patient_id

def generate_patient_data(patient_id, ward):
    producer = Producer(read_ccloud_config("client.properties"))
    json_dict = {}
    for i in range(0, 100):
        for i,j in zip(patient_id, ward):
            json_dict['PatientId'] = i
            json_dict['ward'] = j
            json_dict['data'] = {}
            json_dict['data']['Temperature'] = str(round(random.uniform(lower_limit_temp, upper_limit_temp), 2))
            json_dict['data']['HeartRate'] = str(random.randint(lower_limit_heart, upper_limit_heart))
            json_dict['data']['RespirationRate'] = str(random.randint(lower_limit_resp, upper_limit_resp))
            json_dict['data']['OxygenSaturation'] = str(random.randint(lower_limit_oxy, upper_limit_oxy))
            json_dict['data']['BloodPressure'] = str(random.randint(lower_limit_bp, upper_limit_bp))

            producer.produce("patient_vitals", str(json_dict))
            time.sleep(30)

            # with (open('json_data.json', 'a')) as f:
            #     json.dump(json_dict, f, indent=2)
    producer.flush()
            
def ward_gen():
    ward = ['ward_a', 'ward_b', 'ward_c']
    list_of_wards = list()
    for i in range(0, 30):
        list_of_wards.append(random.choice(ward))
    return list_of_wards

## Convert list of dictionaries to Json
def convert(lst):
    return json.dumps(lst, indent=2)
    
if __name__ == "__main__":
    patient_ids, wards = generate_patientID(), ward_gen()
    generate_patient_data(patient_ids, wards)
        
        
