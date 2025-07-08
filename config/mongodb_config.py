# MONGODB_CONFIG = {
#     'host': 'localhost',
#     'port': 27017,
#     'database': 'property_data',
#     'collections': {
#         'raovat321': 'raovat321',
#         'batdongsan': 'batdongsan',
#         'nhadat247': 'nhadat247',
#         'nhadat24h': 'nhadat24h',
#         'cafeland': 'cafeland',
#     },
#     'uri': 'mongodb://localhost:27017/'
# }

#MongoDb Atlas 
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

MONGODB_CONFIG = {
    'database': 'property_data',
    'collections': {
        'raovat321': 'raovat321',
        'batdongsan': 'batdongsan',
        'nhadat247': 'nhadat247',
        'nhadat24h': 'nhadat24h',
        'cafeland': 'cafeland',
    },
    'uri': 'mongodb+srv://22026532:Abcxyz2004%40@cluster0.shgl2uj.mongodb.net/property_data?retryWrites=true&w=majority'
}
