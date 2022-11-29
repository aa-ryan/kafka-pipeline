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

    risk_of = "Risk of: "
    description = ""

    def check_temperature(temperature):
        if 0 <= temperature < 35:
            description += risk_of + "Severe Hypothermia"
        if temperature >= 39.1:
            description += risk_of + "Severe Hyperthermia"
        if 35 <= temperature <= 35.9:
            description += risk_of + "Moderate Hypothermia"
        if 38.1 <= temperature <= 39:
            description += risk_of + "Moderate Hyperthermia"
        if 36 <= temperature <= 36.9:
            description += risk_of + "Mild Hypothermia"
        if 37.6 <= temperature <= 38:
            description += risk_of + "Mild Hyperthermia"

    def check_heart_rate(heart_rate):
        if 0 <= heart_rate < 40:
            description += "Possible " + risk_of + "Bradyarrhythmias"
        if heart_rate > 130:
            description += "Possible " + risk_of + "Tachyarrhythmias or Atrial Fibrillation"
        if 40 <= heart_rate < 41:
            description += "Possible" + risk_of + "Bradyarrhythmias or Sinus Bradycardia"
        if 111 <= heart_rate <= 130:
            description += "Possible" + risk_of + "Sinus Tachyarrhythmia or Tachyarrhythmias"
        if 41 <= heart_rate <= 50:
            description += "Possible" + risk_of + "Sinus Bradyarrhythmia"
        if 91 <= heart_rate <= 100:
            description += "Possible" + risk_of + "Sinus Tachyarrhythmia"

    def check_respiration_rate(respiration_rate):
        if 0 <= respiration_rate < 8:
            description += risk_of + "Severe Bradyapnea"
        if respiration_rate >= 25:
            description += risk_of + "Severe Tachypnea"
        if 8 <= respiration_rate <= 9:
            description += risk_of + "Moderate Bradyapnea"
        if 21 <= respiration_rate <= 24:
            description += risk_of + "Moderate Tachypnea"
        if 10 <= respiration_rate <= 11:
            description += risk_of + "Mild Bradyapnea"
        if 19 <= respiration_rate <= 20:
            description += risk_of + "Mild Tachypnea"

    def check_blood_oxygen(blood_oxygen):
        if 0 <= blood_oxygen <= 85:
            description += risk_of + "Severe Hypoxia"
        if 86 <= blood_oxygen <= 88:
            description += risk_of + "Moderate Hypoxia"
        if 89 <= blood_oxygen < 92:
            description += risk_of + "Mild Hypoxia"

    def check_blood_pressure(blood_pressure):
        if 0 <= blood_pressure <= 90:
            description += risk_of + "Severe Hypotension"
        if blood_pressure > 220:
            description += risk_of + "Severe Hypertension"
        if 91 <= blood_pressure <= 100:
            description += risk_of + "Moderate Hypotension"
        if 201 <= blood_pressure <= 220:
            description += risk_of + "Moderate Hypertension"
        if 101 <= blood_pressure <= 110:
            description += risk_of + "Mild Hypotension"
        if 181 <= blood_pressure <= 200:
            description += risk_of + "Mild Hypertension"

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

