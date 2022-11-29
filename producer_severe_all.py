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
upper_limit = 1099

# ------Vitals---------------------------------------------

lower_limit_oxy = 85
upper_limit_oxy = 94

lower_limit_heart = 40
upper_limit_heart = 131

lower_limit_bp = 90
upper_limit_bp = 220

lower_limit_resp = 8
upper_limit_resp = 25

lower_limit_temp = 33.00
upper_limit_temp = 40.00



        
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
            time.sleep(1)

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
        
        