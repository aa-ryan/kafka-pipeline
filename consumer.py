#!/usr/bin/env python
# coding: utf-8

# get_ipython().system('pip install confluent-kafka')


from confluent_kafka import Consumer
import random
import json

def check_severity(patient_dict):
    blood_oxygen = int(patient_dict['data']['OxygenSaturation'])
    heart_rate = int(patient_dict['data']['HeartRate'])
    blood_pressure = int(patient_dict['data']['BloodPressure'])
    respiration_rate = int(patient_dict['data']['RespirationRate'])
    temperature = float(patient_dict['data']['Temperature'])

    response_dict = {}
    response_dict['PatientId'] = patient_dict['PatientId']
    response_dict['ward'] = patient_dict['ward']
    
    def check_temperature(temperature):
        if 0 <= temperature < 35 or temperature >= 39.1:
            return "High Risk"
        if 35 <= temperature <= 35.9 or 38.1 <= temperature <= 39:
            return "Moderate Risk"
        if 36 <= temperature <= 36.9 or 37.6 <= temperature <= 38:
            return "Low Risk"
        if 37 <= temperature <= 37.5:
            return "Healthy"
            
    def check_heart_rate(heart_rate):
        if 0 <= heart_rate < 40 or heart_rate > 130:
            return "High Risk"
        if 40 <= heart_rate < 41 or 111 <= heart_rate <= 130:
            return "Moderate Risk"
        if 41 <= heart_rate <= 50 or 91 <= heart_rate <= 100:
            return "Low Risk"
        if 51 <= heart_rate <= 90: 
            return "Healthy"
        
    def check_respiration_rate(respiration_rate):
        if 0 <= respiration_rate < 8 or respiration_rate >= 25:
            return "High Risk"
        if 8 <= respiration_rate <= 9 or 21 <= respiration_rate <= 24:
            return "Moderate Risk"
        if 10 <= respiration_rate <= 11 or 19 <= respiration_rate <= 20:
            return "Low Risk"
        if 12 <= respiration_rate <= 18:
            return "Healthy"
        
        
    def check_blood_oxygen(blood_oxygen):
        if 0 <= blood_oxygen <= 85:
            return "High Risk"
        if 86 <= blood_oxygen <= 88:
            return "Moderate Risk"
        if 89 <= blood_oxygen < 92:
            return "Low Risk"
        if blood_oxygen >= 92:
            return "Healthy"
        
        
    def check_blood_pressure(blood_pressure):
        if 0 <= blood_pressure <= 90 or blood_pressure > 220:
            return "High Risk"
        if 91 <= blood_pressure <= 100 or 201 <= blood_pressure <= 220:
            return "Moderate Risk"
        if 101 <= blood_pressure <= 110 or 181 <= blood_pressure <= 200:
            return "Low Risk"
        if 111 <= blood_pressure <= 180:
            return "Healthy"

    def check_risk(blood_oxygen, heart_rate, blood_pressure, respiration_rate, temperature):
        if (check_blood_oxygen(blood_oxygen) or check_blood_pressure(blood_pressure) or check_heart_rate(heart_rate) or check_respiration_rate(respiration_rate) or check_temperature(temperature)) == "High Risk":
            return "High Risk"
        if (check_blood_oxygen(blood_oxygen) or check_blood_pressure(blood_pressure) or check_heart_rate(heart_rate) or check_respiration_rate(respiration_rate) or check_temperature(temperature)) == "Moderate Risk":
            return "Moderate Risk"
        if (check_blood_oxygen(blood_oxygen) or check_blood_pressure(blood_pressure) or check_heart_rate(heart_rate) or check_respiration_rate(respiration_rate) or check_temperature(temperature)) == "Low Risk":
            return "Mild Risk"
        if (check_blood_oxygen(blood_oxygen) or check_blood_pressure(blood_pressure) or check_heart_rate(heart_rate) or check_respiration_rate(respiration_rate) or check_temperature(temperature)) == "Healthy":
            return "No Risk"

    response_dict['Risk Factor'] = check_risk(blood_oxygen, heart_rate, blood_pressure, respiration_rate, temperature)

    return str(response_dict)

def result_producer(patient_dict):
    from confluent_kafka import Producer
    conf = Producer(read_ccloud_config("client.properties"))
    conf.produce("model_output", check_severity(patient_dict))
    conf.flush()
        
def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

def str_to_dic(s):
    return eval(s)

if __name__ == "__main__":
    props = read_ccloud_config("client.properties")
    props["group.id"] = "python-group-1"
    # print("Group ID - {}".format(pros['group.id']) )
    props["auto.offset.reset"] = "latest"

    consumer = Consumer(props)

    consumer.subscribe(["patient_vitals"])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                # print(msg.value().decode('utf-8'))
                print("Consumed: " + str(str_to_dic(msg.value().decode('utf-8'))))
                result_producer(str_to_dic(msg.value().decode('utf-8')))
            else:
                print("NONE")
    except KeyboardInterrupt:
        print("BYE")
    finally:
        print("Closing Consumer")
        consumer.close()

