# PyMongo Test

import argparse

from pymongo import MongoClient

parser = argparse.ArgumentParser()
parser.add_argument('host', type=str, help='MongoDB IP')
parser.add_argument('-d', '--database', type=str, default='chair', help='DB name, default chair')
parser.add_argument('-c', '--collection', type=str, default='big', help='Collection name, default big')

args = parser.parse_args()

# Connect DB
client = MongoClient(host=args.host)
print(f'original dbs: {client.list_database_names()}')

# Create Database, Collection, and Insert Documents
collection = client[args.database][args.collection]
doc = {'_id': '001', 'size': 'L', 'price': 100}
print(f'\ninsert_one {doc} into {collection.full_name}')
r = collection.insert_one(doc)
print(f'document {r.inserted_id} is inserted')

print(f'\n[{collection.full_name}]: {collection.count_documents({})}')
for x in collection.find():
    print(x)

print(f'current dbs: {client.list_database_names()}')


# Insert Multiple Documents
docs = [{'_id': '002', 'size': 'S', 'price': 20},
        {'_id': '003', 'size': 'M', 'price': 300},
        {'_id': '004', 'size': 'S', 'price': 250},
        {'_id': '005', 'size': 'M', 'price': 324},
        {'_id': '006', 'size': 'M', 'price': 310},
        {'_id': '007', 'size': 'L', 'price': 422},
        {'_id': '008', 'size': 'L', 'price': 500},
       ]
print(f'\ninsert_many {docs} into {collection.full_name}')
r = collection.insert_many(docs)
print(f'{len(r.inserted_ids)} documents are inserted')

print(f'\n[{collection.full_name}]: {collection.count_documents({})}')
for x in collection.find():
    print(x)


# Find One Document
condition = {'size': 'S'}
print(f'\nfind_one {condition} from {collection.full_name}')
r = collection.find_one(condition)
print(f'document {r} is found')

print(f'\n[{collection.full_name}]: {collection.count_documents({})}')
for x in collection.find():
    print(x)


# Find with Filter
condition = {'price': {'$gt': 300 }}
print(f'\nfind {condition} from {collection.full_name}')
r = collection.find(condition)
for x in r:
    print(x)

print(f'\n[{collection.full_name}]: {collection.count_documents({})}')
for x in collection.find():
    print(x)

# Update Many
condition = {'price': {'$gt': 300 }}
value = {'$set': {'note': 'expensive'}}
print(f'\nupdate_many {condition} {value} from {collection.full_name}')
r = collection.update_many(filter=condition, update=value)
print(f'{r.matched_count} documents matched and {r.modified_count} documents modified')

print(f'\n[{collection.full_name}]: {collection.count_documents({})}')
for x in collection.find():
    print(x)


# Delete One Document
condition = {'size': 'S'}
print(f'\ndelete_one {condition} from {collection.full_name}')
r = collection.delete_one(condition)
print(f'{r.deleted_count} document is deleted')

print(f'\n[{collection.full_name}]: {collection.count_documents({})}')
for x in collection.find():
    print(x)


# Delete Many Documents
condition = {'size': 'M'}
print(f'\ndelete_many {condition} from {collection.full_name}')
r = collection.delete_many(condition)
print(f'{r.deleted_count} documents are deleted')

print(f'\n[{collection.full_name}]: {collection.count_documents({})}')
for x in collection.find():
    print(x)


# Drop DB
print(f'\ndrop_database {collection.database.name}')
client.drop_database(collection.database.name)

print(f'\ncurrent dbs: {client.list_database_names()}')


# Close Connection
print('\ncolse connection')
client.close()
