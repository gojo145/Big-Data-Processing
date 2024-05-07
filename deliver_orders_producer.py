from kafka import KafkaProducer
import json

# Read data from data.json
with open('data.json') as f:
    orders = json.load(f)

# Filter orders based on type
inventory_orders = [order for order in orders if order['type'] == 'inventory']
delivery_orders = [order for order in orders if order['type'] == 'delivery']


# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Publish delivery orders
for order in delivery_orders:
    producer.send('delivery_orders', value=order)

# Close the producer
producer.close()
