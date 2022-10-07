from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd
from uuid import uuid4


FILE_PATH = "restaurant_orders.csv"
columns=['OrderNumber', 'OrderDate', 'ItemName', 'Quantity', 'ProductPrice', 'TotalProducts']

API_KEY = 'I57XO5UL5QJCDYQY'
ENDPOINT_SCHEMA_URL  = 'https://psrc-mw731.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'hIS/RPzQsDjoYj6knk4P6IpQPYYaybVvyEGJI/8v7/Ew3OOCNsLngJc4YSsxsHAt'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'BLMDVYK32ET3T65C'
SCHEMA_REGISTRY_API_SECRET = 'KRANuQTKIQtXP2j1Co5TjcAzxhInwDpBNLiyKunxXRDUiu1gXKMzovdF3+ULsqgx'


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


def order_to_dict(order, ctx):
    return dict(OrderNumber=order.OrderNumber,
                OrderDate=order.OrderDate,
                ItemName=order.ItemName,
                Quantity=order.Quantity,
                ProductPrice=order.ProductPrice,
                TotalProducts=order.TotalProducts)





def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY,
                'batch.size': 10000,
                #'batch.num.messages': 5,
                'linger.ms': 1000
                }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,
            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
            }


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    print(schema_registry_client.get_subjects())
    print(schema_registry_client.get_latest_version('restaurent-take-away-data-key').schema)
    print(schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str)
    latest_schema_str = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(latest_schema_str, schema_registry_client, order_to_dict)

    producer = Producer(sasl_conf())

    df = pd.read_csv(FILE_PATH)
    df = df.iloc[1:, :]
    for data in df.values:
        order_data_dict = dict(zip(columns, data))
        order_data = Order(order_data_dict['OrderNumber'], order_data_dict['OrderDate'],
                           order_data_dict['ItemName'], order_data_dict['Quantity'],
                           order_data_dict['Quantity'], order_data_dict['TotalProducts'])
        # print(order_data_dict)
        try:
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()), order_to_dict),
                             value=json_serializer(order_data, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
            producer.poll(0)
        except BufferError:
            producer.flush()
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()), order_to_dict),
                             value=json_serializer(order_data, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
        # break
    producer.flush()


if __name__== "__main__":
    main('restaurent-take-away-data')