import json

# Read data from data.json
with open('data.json') as f:
    orders = json.load(f)

# Filter orders based on type
inventory_orders = [order for order in orders if order['type'] == 'inventory']
delivery_orders = [order for order in orders if order['type'] == 'delivery']
