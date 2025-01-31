from db import mongoDb
from aggregations import *

mongo = mongoDb()
db = mongo.db

while True:
    souhait = input(
        """Souhaitez-vous: 
        - Afficher la nombre d'entrées par villes pour des taxis en mouvement sur la dernière heure (A) 
        - Afficher le taxi qui a fait le plus d'arrets (B)
        - Afficher l'entreprise ayant le plus de véhicules avec une autonomie de 5 (C)
        - Afficher la moyenne d'age de la flotte de chaque entreprise (D)
        - Afficher Les villes avec les taxis qui font le plus d'arrets (E)
        - Identifier les taxis à l'arret (F)
        
        [Q pour quitter] ?
        """)
    
    if souhait.lower() == "a":
        aggregation_1(db)
    elif souhait.lower() == "b":
        aggregation_2(db)
    elif souhait.lower() == "c":
        aggregation_3(db)
    elif souhait.lower() == "d":
        aggregation_4(db)
    elif souhait.lower() == "e":
        aggregation_6(db)
    elif souhait.lower() == "f":
        aggregation_7(db)
    elif souhait.lower() == "q":
        print("Au revoir !")
        break

    else:
        print("Merci d'entrer A, B, C, D, E, F ou Q.")
        