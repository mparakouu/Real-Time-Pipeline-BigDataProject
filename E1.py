from uxsim import *
import itertools
import pandas as pd
from kafka import KafkaProducer
import json
import time
import datetime
from pyspark.sql import SparkSession


seed = None


W = World(
    name="",
    deltan=5,
    tmax=3600, #1 hour simulation
    print_mode=1, save_mode=0, show_mode=1,
    random_seed=seed,
    duo_update_time=600
)
random.seed(seed)

# network definition
"""
    N1  N2  N3  N4 
    |   |   |   |
W1--I1--I2--I3--I4-<E1
    |   |   |   |
    v   ^   v   ^
    S1  S2  S3  S4
"""

# χαρακτηριστικά δικτύου κυκλοφορίας
signal_time = 20
sf_1=1
sf_2=1

# δημιουργία κόμβων για τις διασταυρώσεις και τους δρόμους
I1 = W.addNode("I1", 1, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I2 = W.addNode("I2", 2, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I3 = W.addNode("I3", 3, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I4 = W.addNode("I4", 4, 0, signal=[signal_time*sf_1,signal_time*sf_2])
W1 = W.addNode("W1", 0, 0)
E1 = W.addNode("E1", 5, 0)
N1 = W.addNode("N1", 1, 1)
N2 = W.addNode("N2", 2, 1)
N3 = W.addNode("N3", 3, 1)
N4 = W.addNode("N4", 4, 1)
S1 = W.addNode("S1", 1, -1)
S2 = W.addNode("S2", 2, -1)
S3 = W.addNode("S3", 3, -1)
S4 = W.addNode("S4", 4, -1)


# οι συνδέσεις των κόμβων για την κίνηση των αμαξιών
#E <-> W direction: signal group 0
for n1,n2 in [[W1, I1], [I1, I2], [I2, I3], [I3, I4], [I4, E1]]:
    W.addLink(n2.name+n1.name, n2, n1, length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3, signal_group=0)
    
#N -> S direction: signal group 1
for n1,n2 in [[N1, I1], [I1, S1], [N3, I3], [I3, S3]]:
    W.addLink(n1.name+n2.name, n1, n2, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

#S -> N direction: signal group 2
for n1,n2 in [[N2, I2], [I2, S2], [N4, I4], [I4, S4]]:
    W.addLink(n2.name+n1.name, n2, n1, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)
    


# random demand definition every 30 seconds
dt = 30
demand = 2 #average demand for the simulation time
demands = []
for t in range(0, 3600, dt):
    dem = random.uniform(0, demand)
    for n1, n2 in [[N1, S1], [S2, N2], [N3, S3], [S4, N4]]:
        W.adddemand(n1, n2, t, t+dt, dem*0.25)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})
    for n1, n2 in [[E1, W1], [N1, W1], [S2, W1], [N3, W1],[S4, W1]]:
        W.adddemand(n1, n2, t, t+dt, dem*0.75)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})

W.exec_simulation()

W.analyzer.print_simple_stats()


# μετατροπή των data των αυτοκινήτων σε πλαίσιο δεδομένων pandas
vehicles_data = W.analyzer.vehicles_to_pandas()

# save σε ένα αρχείο CSV
vehicles_data.to_csv('vehicles_data.csv', index=False, columns=['name', 'dn', 'orig', 'dest', 't', 'link', 'x', 's', 'v'])


# Χρονική σφραγίδα --> όταν ξεκινήσει η προσομοίωση
start_time = datetime.datetime.now()

# μετατροπή των data των οχημάτων -- > json
def vehicle_to_json(vehicle_data, start_time):
    # χρόνος = χρονική σφραγίδα + t
    simulated_time = start_time + datetime.timedelta(seconds=vehicle_data["t"])
    data = {
        "name": vehicle_data["name"],
        "orig": vehicle_data["orig"],
        "dest": vehicle_data["dest"],
        "time": simulated_time.strftime('%Y-%m-%d %H:%M:%S'),  
        "link": vehicle_data["link"],
        "position": vehicle_data["x"],
        "spacing": vehicle_data["s"],
        "speed": vehicle_data["v"]
    }
    return json.dumps(data)

# εκκίνηση Kafka Producer
#Kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
Kafka_producer = KafkaProducer(bootstrap_servers='150.140.142.71:9092')

# save οχήματα με ίδιο t
vehicles_same_time = {}

# οχήματα με waiting at origin node
waiting_node_vehicles = []

# ανά Ν στέλντοναι data
N = 5  
for index, row in vehicles_data.iterrows():
    # εάν το όχημα δεν κινείται  
    if row['link'] != 'E1':  
        current_time = row['t']
        # εάν ο χρόνος του δεν υπάρχει ήδη στο vehicles_same_time, την προσθέτει 
        if current_time not in vehicles_same_time:
            vehicles_same_time[current_time] = []
            # προσθήκη αυτών με ίδιο χρόνο στην ίδια λίστα 
        vehicles_same_time[current_time].append(row)
        
        # περίπτωση --> waiting at origin node -- > προσθήκη στην λίστα των waiting at origin node
        if row['link'] == 'waiting_at_origin_node':
            waiting_node_vehicles.append(row)
        else:
            # για όχημα με μοναδικό χρόνο --> το στέλνουμε 
            if len(vehicles_same_time[current_time]) == 1:
                vehicle_json = vehicle_to_json(row, start_time)
                # σε json --> στέλνει 
                Kafka_producer.send('vehicle_positions', vehicle_json.encode('utf-8'))
                print(vehicle_json)
            else:
                # για τα οχήματα με τον ίδιο χρόνο --> σε json --> στέλνει 
                for vehicle in vehicles_same_time[current_time]:
                    vehicle_json = vehicle_to_json(vehicle, start_time)
                    Kafka_producer.send('vehicle_positions', vehicle_json.encode('utf-8'))
                    print(vehicle_json)

    # όταν φτάσει η σειρά του waiting node --> json --> στέλνει 
    for vehicle in waiting_node_vehicles:
        # current_time = row['t']
        if vehicle['t'] == current_time:
            vehicle_json = vehicle_to_json(vehicle, start_time)
            Kafka_producer.send('vehicle_positions', vehicle_json.encode('utf-8'))
            print(vehicle_json)
            # αφαίρεση από waiting_node_vehicles
            waiting_node_vehicles.remove(vehicle) 
            
    time.sleep(N)  

Kafka_producer.close()

