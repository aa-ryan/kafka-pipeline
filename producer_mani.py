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
lower_limit_oxy = 92
upper_limit_oxy = 98

lower_limit_heart = 132
upper_limit_heart = 138

lower_limit_bp = 111
upper_limit_bp = 180

lower_limit_resp = 12
upper_limit_resp = 18

lower_limit_temp = 37.00
upper_limit_temp = 37.50



patient = [1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018,1019,1020,1021,1022,1023,1024,1025,1026,1027,1028,1029,1030]

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
            time.sleep(40)

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
    wards = ward_gen()
    patient = [1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018,1019,1020,1021,1022,1023,1024,1025,1026,1027,1028,1029,1030]
    generate_patient_data(patient, wards)
