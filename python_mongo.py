# importing the required modules  
from json import loads  
from kafka import KafkaConsumer  
from pymongo import MongoClient  

my_consumer = KafkaConsumer(  
    'testnum',  
     bootstrap_servers = ['localhost : 9092'],  
     auto_offset_reset = 'earliest',  
     enable_auto_commit = True,  
     group_id = 'my-group',  
     value_deserializer = lambda x : loads(x.decode('utf-8'))  
     )  


my_client = MongoClient('localhost : 27017')  
my_collection = my_client.testnum.testnum  

for message in my_consumer:  
    message = message.value  
    collection.insert_one(message)  
    print(message + " added to " + my_collection)  



# 
from pymongo import MongoClient # import mongo client to connect  
import pprint  
# Creating instance of mongoclient  
client = MongoClient()  
# Creating database  
db = client.javatpoint  
employee = {"id": "101",  
"name": "Peter",  
"profession": "Software Engineer",  
}  
# Creating document  
employees = db.employees  
# Inserting data  
employees.insert_one(employee)  
# Fetching data  
#find( { "name.last": "Hopper" } )
pprint.pprint(employees.find_one())  


# 
from pyhive import presto  # or import hive or import trino
cursor = presto.connect('localhost').cursor()
cursor.execute('SELECT * FROM my_awesome_data LIMIT 10')
print cursor.fetchone()
print cursor.fetchall()


