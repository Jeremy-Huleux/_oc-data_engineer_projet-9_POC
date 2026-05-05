import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# 1. Configuration du producteur
# value_serializer transforme automatiquement notre dictionnaire en JSON puis en Bytes
producer = KafkaProducer(
    bootstrap_servers=['redpanda:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'client_tickets'

# 2. Fonction de génération des tickets
def generate_ticket():
    types = ['Technique', 'Facturation', 'Commercial', 'Autre']
    priorities = ['Basse', 'Moyenne', 'Haute', 'Urgent']
    
    return {
        "ticket_id": str(random.randint(10000, 99999)),
        "customer_id": f"CUST-{random.randint(100, 999)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "request": "Aide demandée pour le service",
        "type": random.choice(types),
        "priority": random.choice(priorities)
    }

print(f"Démarrage de l'envoi sur le topic : {TOPIC}...")

# 3. Boucle infinie pour simuler le flux temps réel
try:
    while True:
        ticket = generate_ticket()
        producer.send(TOPIC, value=ticket)
        print(f"Ticket envoyé : ID={ticket['ticket_id']} | Type={ticket['type']}")
        time.sleep(2) # Pause de 2 secondes entre chaque envoi
except KeyboardInterrupt:
    print("\nArrêt du producteur.")
finally:
    producer.close()