import json
from kafka import KafkaProducer, KafkaConsumer

# Function to read data from data.json and filter orders based on type
def process_data():
    with open('data.json') as f:
        orders = json.load(f)
    inventory_orders = [order for order in orders if order['type'] == 'inventory']
    delivery_orders = [order for order in orders if order['type'] == 'delivery']
    return inventory_orders, delivery_orders

# Function to publish inventory orders to Kafka
def publish_inventory_orders(orders):
    bootstrap_servers = ['localhost:9092']
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    for order in orders:
        producer.send('inventory_orders', value=order)
    producer.close()

# Function to publish delivery orders to Kafka
def publish_delivery_orders(orders):
    bootstrap_servers = ['localhost:9092']
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    for order in orders:
        producer.send('delivery_orders', value=order)
    producer.close()

# Function to consume and process inventory orders from Kafka
def consume_inventory_orders():
    bootstrap_servers = ['localhost:9092']
    consumer = KafkaConsumer('inventory_orders',
                             bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='latest',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        order = message.value
        # Process inventory orders
        print("Processing inventory order:", order)
    consumer.close()

# Function to consume and process delivery orders from Kafka
def consume_delivery_orders():
    bootstrap_servers = ['localhost:9092']
    consumer = KafkaConsumer('delivery_orders',
                             bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='latest',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        order = message.value
        # Process delivery orders
        print("Processing delivery order:", order)
    consumer.close()

# Main function to run the entire system
def main():
    # Process data
    inventory_orders, delivery_orders = process_data()
    
    # Publish orders to Kafka
    publish_inventory_orders(inventory_orders)
    publish_delivery_orders(delivery_orders)
    
    # Consume and process orders from Kafka
    consume_inventory_orders()
    consume_delivery_orders()

if __name__ == "__main__":
    main()
