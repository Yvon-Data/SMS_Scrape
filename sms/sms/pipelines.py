# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo


class SmsPipeline:
    def process_item(self, item, spider):
        return item


class MongoDb_VIN(object):

    collection_name = "Carriers"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri, ssl=True)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('State_Inspection'):
            self.db[self.collection_name].update_one(
                {'_id': item['USDOT']},
                {
                    '$set': {
                        'State_Registered': item['State_Registered'],
                        'State_Inspection': item['State_Inspection'],
                        'State_Crash': item['State_Crash']
                    }
                },
                upsert=True
            )
            return item

        elif adapter.get('Vehicle_GVWR'):
            self.db[self.collection_name].update_one(
                {
                    '_id': item['USDOT'],
                    'VIN_Check': {'$ne': item['VIN']}
                 },
                {
                    '$inc': {adapter.get('Vehicle_GVWR'): 1},
                    '$addToSet': {
                        'VIN_Check': item['VIN'],
                        'VIN': item
                    }
                },
                upsert=True
            )
            return item

        else:
            self.db[self.collection_name].update_one(
                {'_id': item['USDOT']},
                {'$addToSet': {'VIN': item}},
                upsert=True
            )

            return item


class MongoDb_Details(object):

    collection_name = "Carriers"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri, ssl=True)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        self.db[self.collection_name].update_one(
            {'_id': item['USDOT']},
            {
                '$set': item
            },
            upsert=True
        )
        return item
