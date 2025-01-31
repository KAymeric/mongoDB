from pymongo import MongoClient
from env import env

    
class mongoDb:
    def __init__(self):
        self.env = env()
        self.client = MongoClient(self.env.CONNECTION_STRING)
        self._db = self.client[self.env.DB_NAME]
     
    @property
    def db(self):
        self.initCollections()
        return self._db

    def initCollections(self):
        collections = self._db.list_collection_names()
        
        if "taxis" not in collections:
            self._db.create_collection("taxis", validator = {
                "$jsonSchema" : {
                    "bsonType" : "object",
                    "required" : ["name", "city", "address", "founded_year", "fleet_size", "taxis"],
                    "properties" : {
                        "name" : {
                            "bsonType" : "string"
                        },
                        "city" : {
                            "bsonType" : "string"
                        },
                        "address" : {
                            "bsonType" : "string"
                        },
                        "founded_year" : {
                            "bsonType" : "int"
                        },
                        "fleet_size" : {
                            "bsonType" : "int"
                        },
                        "contact_email" : {
                            "bsonType" : "string",
                        },
                        "contact_phone" : {
                            "bsonType" : "string",
                        },
                        "taxis" : {
                            "bsonType" : "array",
                            "items" : {
                                "bsonType" : "object",
                                "required" : ["autonomy_level", "year_of_production", "license_plate", "manufacturer" ,"battery_capacity_kwh", "current_status","location"],
                                "properties" : {
                                    "taxi_id" : {
                                        "bsonType" : "string"
                                    },
                                    "autonomy_level" : {
                                        "bsonType" : "int"
                                    },
                                    "year_of_production" : {
                                        "bsonType" : "int"
                                    },
                                    "license_plate" : {
                                        "bsonType" : "string"
                                    },
                                    "manufacturer" : {
                                        "bsonType" : "string"
                                    },
                                    "battery_capacity_kwh" : {
                                        "bsonType" : "int"
                                    },
                                    "current_status" : {
                                        "bsonType" : "string",
                                    },
                                    "location" : {
                                        "bsonType" : "object",
                                        "required" : ["lat", "lon"],
                                        "properties" : {
                                            "lat" : {
                                                "bsonType" : "double"
                                            },
                                            "lon" : {
                                                "bsonType" : "double"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })
            
        if "gps" not in collections:
            self._db.create_collection(
                "gps",
                timeseries={
                    "timeField": "timestamp",
                    "metaField": "license_plate",
                    "granularity": "seconds"
                }
            )
            self._db["gps"].create_index("license_plate")