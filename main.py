import asyncio
from asyncua import Client, ua
from datetime import datetime, timedelta

# URL of the OPC UA server
url = "opc.tcp://localhost:62541"

# NodeIds of the read and alert nodes
node_id_read = "ns=1;s=[Sample_Device]_Meta:Random/RandomInteger1"
node_id_alert = "ns=1;s=[Sample_Device]_Meta:Writeable/WriteableString1"

# Simulated ML model class
class MLModel:
    def __init__(self, threshold=50):
        self.threshold = threshold

    def predict(self, value):
        # Simulates a prediction based on the threshold
        if value > self.threshold:
            return f"Possible failure ({int(value)})"
        else:
            return "Normal operation"

    async def process_historical_data(self, historical_data):
        # Simulates processing historical data (does nothing for now)
        for data in historical_data:
            print(f"Processing historical value: {data}")

# Subscription handler class
class SubHandler:
    def __init__(self, alert_node, model):
        # Alert node where messages will be sent
        self.alert_node = alert_node
        self.model = model

    # Method to handle data change notifications
    def datachange_notification(self, node, val, data):
        print(f"Data change on node {node}: {val}")
        # Use the ML model to generate an alert message based on the value
        alert_message = self.model.predict(val)
        # Send the alert message to the SCADA
        asyncio.create_task(self.send_alert(alert_message))

    # Send the alert message to the alert node in SCADA
    async def send_alert(self, message):
        variant_value = ua.Variant(message, ua.VariantType.String)
        await self.alert_node.write_value(variant_value)
        print(f"Message '{message}' written to {node_id_alert}")

# Function to read historical data
async def read_historical_data(client, node_id, start_time, end_time):
    try:
        node = client.get_node(node_id)
        historical_data = await node.read_raw_history(start_time, end_time)
        return historical_data
    except ua.UaStatusCodeError as e:
        print(f"Failed to read historical data: {e}")
        return []

# Main asynchronous function
async def main():
    print(f"Connecting to {url} ...")
    async with Client(url=url) as client:
        # Get the alert node
        alert_node = client.get_node(node_id_alert)
        
        # Create the ML model
        model = MLModel(threshold=50)
        
        # Read historical data from the last minute
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=1)
        historical_data = await read_historical_data(client, node_id_read, start_time, end_time)
        
        # Process historical data with the ML model
        await model.process_historical_data(historical_data)
        
        # Create a subscription handler with the alert node and ML model
        handler = SubHandler(alert_node, model)
        
        # Create a subscription to the OPC UA server
        subscription = await client.create_subscription(500, handler)
        
        # Get the read node
        node_read = client.get_node(node_id_read)
        
        # Subscribe to the read node
        handle = await subscription.subscribe_data_change(node_read)

        print(f"Subscribed to {node_id_read}, waiting for data changes...")

        try:
            while True:
                await asyncio.sleep(1)  # Keep the subscription active
        finally:
            # Unsubscribe and delete the subscription when finished
            await subscription.unsubscribe(handle)
            await subscription.delete()

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
