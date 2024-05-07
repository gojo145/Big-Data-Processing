from kafka import KafkaConsumer
import json

# Read data from data.json
with open('data.json') as f:
    orders = json.load(f)

# Filter orders based on type
inventory_orders = [order for order in orders if order['type'] == 'inventory']
delivery_orders = [order for order in orders if order['type'] == 'delivery']



# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Create Kafka consumer
consumer = KafkaConsumer('delivery_orders',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='latest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Process delivery orders
for message in consumer:
    order = message.value
    # Perform actions such as scheduling deliveries, updating delivery status, notifying customers, etc.
    print("Processing delivery order:", order)

# Close the consumer
consumer.close()
