from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from kafka_producer import Order
from kafka_producer import order_to_dict

API_KEY = 'I57XO5UL5QJCDYQY'
ENDPOINT_SCHEMA_URL  = 'https://psrc-mw731.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'hIS/RPzQsDjoYj6knk4P6IpQPYYaybVvyEGJI/8v7/Ew3OOCNsLngJc4YSsxsHAt'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'BLMDVYK32ET3T65C'
SCHEMA_REGISTRY_API_SECRET = 'KRANuQTKIQtXP2j1Co5TjcAzxhInwDpBNLiyKunxXRDUiu1gXKMzovdF3+ULsqgx'




def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY,
                 'fetch.min.bytes': 65536,
                 # 'receive.buffer.bytes': 65536
                 # 'fetch.max.wait.ms': 2000
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }


class Order(object):

    def __init__(self, orderNumber, orderDate, itemName, quantity, productPrice, totalProducts):
        self.OrderNumber = orderNumber
        self.OrderDate = orderDate
        self.ItemName = itemName
        self.Quantity = quantity
        self.ProductPrice = productPrice
        self.TotalProducts = totalProducts

    def dict_to_order(dict_order, ctx):
        return Order(dict_order['OrderNumber'], dict_order['OrderDate'],
                     dict_order['ItemName'], dict_order['Quantity'],
                     dict_order['Quantity'], dict_order['TotalProducts'])


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    latest_schema_str = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str
    json_deserializer = JSONDeserializer(latest_schema_str,
                                         from_dict=Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    count_kafka_consumer1_same_group = 0

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if order is not None:
                print("User record {}: order Number: {}\n"
                      .format(msg.key(), order.OrderNumber))
                count_kafka_consumer1_same_group += 1
        except KeyboardInterrupt:
            break

    consumer.close()
    print("Total Consumed by same group consumer2 : " + str(count_kafka_consumer1_same_group))

main("restaurent-take-away-data")
