from datetime import datetime, timedelta

def aggregation_1(db):
    pipeline = [
        {
            "$match": {
                "status": "moving",
                "timestamp": {
                    "$gte": datetime.now() - timedelta(hours=2)
                }
            }
        },
        {
            "$lookup": {
                'from': 'taxis', 
                'localField': 'license_plate', 
                'foreignField': 'taxis.license_plate', 
                'as': 'target_society',
                'pipeline': [
                    {
                        "$unwind": "$taxis"
                    },
                ],
            }
        },
        {
            '$unwind': {
                'path': '$target_society'
            }
        },
        {
            "$group": {
                "_id": "$target_society.city",
                "count": {
                    "$sum": 1
                }
            }
        },  
    ]
    results = db.gps.aggregate(pipeline)
    for r in list(results):
        print(f"{r['_id']}: {r['count']} mouvements de taxis sur la dernière heure")
        
def aggregation_2(db):
    pipeline = [
        {
            "$match": {
                "status": "stopped",
            },
        },
        {
            "$lookup": {
                'from': 'taxis', 
                'localField': 'license_plate', 
                'foreignField': 'taxis.license_plate', 
                'as': 'society',
                'pipeline': [
                    {
                        "$unwind": "$taxis"
                    },
                ],
            }
        },
        {
            '$unwind': {
                'path': '$society'
            }
        },
        {
            "$group": {
                "_id": "$license_plate",
                "count": {
                    "$sum": 1
                },
                "society": {
                    "$first" : "$society"
                }
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
        {
            "$limit": 1
        }, 
    ]
    results = db.gps.aggregate(pipeline)
    for r in list(results):
        print(f"le taxi : {r['_id']} de l'entreprise {r['society']['name']} ({r['society']['city']}, {r['society']['address']}) a effectué {r['count']} arrets")
        
def aggregation_3(db):
    pipeline = [
        {
            "$lookup": {
                'from': 'taxis', 
                'localField': 'license_plate', 
                'foreignField': 'taxis.license_plate', 
                'as': 'society',
                'pipeline': [
                    {
                        "$unwind": "$taxis"
                    },
                ],
            }
        },
        {
            "$match": {
                "society.taxis.autonomy_level": 5
            }
        },
        {
            '$unwind': {
                'path': '$society'
            }
        },
        {
            "$group": {
                "_id": "$society.name",
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
        {
            "$limit": 1
        }
    ]
    results = db.gps.aggregate(pipeline)
    for r in list(results):
        print(f"{r['_id']} est l'entreprise ayant le plus de véhicules avec une autonomie de 5")
        
def aggregation_4(db):
    pipeline = [
        {
            "$lookup": {
                'from': 'taxis', 
                'localField': 'license_plate', 
                'foreignField': 'taxis.license_plate', 
                'as': 'society',
                'pipeline': [
                    {
                        "$unwind": "$taxis"
                    },
                ],
            }
        },
        {
            '$unwind': {
                'path': '$society'
            }
        },
        {
            "$group": {
                "_id": "$society.name",
                "average": {
                    "$avg": {
                        "$subtract": [datetime.now().year, "$society.taxis.year_of_production"]
                    }
                }
            }
        }
    ]
    results = db.gps.aggregate(pipeline)
    for r in list(results):
        print(f"L'entreprise {r['_id']} a une moyenne d'age de {round(r['average'], 2)} ans")
        
def aggregation_6(db):
    pipeline = [
        {
            "$match": {
                "status": "stopped",
            },
        },
        {
            "$lookup": {
                'from': 'taxis', 
                'localField': 'license_plate', 
                'foreignField': 'taxis.license_plate', 
                'as': 'society',
                'pipeline': [
                    {
                        "$unwind": "$taxis"
                    },
                ],
            }
        },
        {
            '$unwind': {
                'path': '$society'
            }
        },
        {
            "$group": {
                "_id": "$society.city",
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
    ]
    results = db.gps.aggregate(pipeline)
    for r in list(results):
        print(f"les taxis ont fait {r['count']} arrets à {r['_id']}")
        
def aggregation_7(db):
    pipeline = [
        {
            "$lookup": {
                'from': 'taxis', 
                'localField': 'license_plate', 
                'foreignField': 'taxis.license_plate', 
                'as': 'society',
                'pipeline': [
                    {
                        "$unwind": "$taxis"
                    },
                ],
            }
        },
        {
            '$unwind': {
                'path': '$society'
            }
        },
        {
            '$sort': {
                'timestamp': -1
            }
        },
        {
            '$group': {
                '_id': '$license_plate',
                'last_status': {
                    '$first': '$status'
                },
                'last_timestamp': {
                    '$first': '$timestamp'
                },
                'taxi_id': {'$first': '$license_plate'},
                'location': {
                    '$first' : {
                        'lat': '$lat',
                        'lon': '$lon',
                    }
                },
                'company_name': {'$first': '$society.name'},
                'company_city': {'$first': '$society.city'},
                'company_fleet_size': {'$first': '$society.fleet_size'},
            }
        },
        {
            '$match': {
                'last_status': 'stopped'
            }
        }
    ]
    
    db["stopped_taxis"].drop()
    db.create_collection(
        'stopped_taxis',
        viewOn='gps',
        pipeline=pipeline
    )