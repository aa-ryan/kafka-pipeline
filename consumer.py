#!/usr/bin/env python
# coding: utf-8

# get_ipython().system('pip install confluent-kafka')


from confluent_kafka import Consumer, Producer
import random
import json

def check_severity(patient_dict):
    description = "Risk Of "
    blood_oxygen = int(patient_dict['data']['OxygenSaturation'])
    heart_rate = int(patient_dict['data']['HeartRate'])
    blood_pressure = int(patient_dict['data']['BloodPressure'])
    respiration_rate = int(patient_dict['data']['RespirationRate'])
    temperature = float(patient_dict['data']['Temperature'])

    response_dict = {}
    response_dict['PatientId'] = patient_dict['PatientId']
    response_dict['ward'] = patient_dict['ward']


    def check_temperature(temperature):
        nonlocal description
        if 0 <= temperature < 35:
            description +=  " Severe Hypothermia, "
        if temperature >= 39.1:
            description +=  " Severe Hyperthermia, "
        if 35 <= temperature <= 35.9:
            description += " Moderate Hypothermia, "
        if 38.1 <= temperature <= 39:
            description += " Moderate Hyperthermia, "
        if 36 <= temperature <= 36.9:
            description += " Mild Hypothermia, "
        if 37.6 <= temperature <= 38:
            description += " Mild Hyperthermia, "

    def check_heart_rate(heart_rate):
        nonlocal description
        if 0 <= heart_rate < 40:
            description += " Possible risk of Bradyarrhythmias, "
        if heart_rate > 130:
            description += " Possible risk of Tachyarrhythmias or Atrial Fibrillation, "
        if 40 <= heart_rate < 41:
            description += " Possible risk of Bradyarrhythmias or Sinus Bradycardia, "
        if 111 <= heart_rate <= 130:
            description += " Possible risk of Sinus Tachyarrhythmia or Tachyarrhythmias, "
        if 41 <= heart_rate <= 50:
            description += " Possible risk of Sinus Bradyarrhythmia, "
        if 91 <= heart_rate <= 110:
            description += "Possible risk of Sinus Tachyarrhythmia, "

    def check_respiration_rate(respiration_rate):
        nonlocal description
        if 0 <= respiration_rate < 8:
            description += " Severe Bradyapnea, "
        if respiration_rate >= 25:
            description += " Severe Tachypnea, "
        if 8 <= respiration_rate <= 9:
            description += " Moderate Bradyapnea, "
        if 21 <= respiration_rate <= 24:
            description += " Moderate Tachypnea, "
        if 10 <= respiration_rate <= 11:
            description += " Mild Bradyapnea, "
        if 19 <= respiration_rate <= 20:
            description += " Mild Tachypnea, "

    def check_blood_oxygen(blood_oxygen):
        nonlocal description
        if 0 <= blood_oxygen <= 85:
            description += " Severe Hypoxia, "
        if 86 <= blood_oxygen <= 88:
            description += " Moderate Hypoxia, "
        if 89 <= blood_oxygen < 92:
            description += " Mild Hypoxia, "

    def check_blood_pressure(blood_pressure):
        nonlocal description
        if 0 <= blood_pressure <= 90:
            description += " Severe Hypotension, "
        if blood_pressure > 220:
            description += " Severe Hypertension, "
        if 91 <= blood_pressure <= 100:
            description += " Moderate Hypotension, "
        if 201 <= blood_pressure <= 220:
            description += " Moderate Hypertension, "
        if 101 <= blood_pressure <= 110:
            description += " Mild Hypotension, "
        if 181 <= blood_pressure <= 200:
            description += " Mild Hypertension, "

    def check_risk(blood_oxygen, heart_rate, blood_pressure, respiration_rate, temperature):
        if "Severe" in description or heart_rate < 40 or heart_rate > 130:
            return "High Risk"
        if "Moderate" in description or 40 <= heart_rate <= 41 or 111 <= heart_rate <= 130:
            return "Moderate Risk"
        if "Mild" in description or 41 <= heart_rate <= 50 or 91 <= heart_rate <= 110:
            return "Mild Risk"

    check_blood_oxygen(blood_oxygen)
    check_heart_rate(heart_rate)
    check_blood_pressure(blood_pressure)
    check_respiration_rate(respiration_rate)
    check_temperature(temperature)

    if check_risk(blood_oxygen, heart_rate, blood_pressure, respiration_rate, temperature) == None:
        return None
    else:
        response_dict['Risk Factor'] = check_risk(blood_oxygen, heart_rate, blood_pressure, respiration_rate, temperature)
        response_dict['Description'] = description
        return str(response_dict)

def result_producer(patient_dict):
    conf = Producer(read_ccloud_config("client.properties"))
    if check_severity(patient_dict) != None:
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

