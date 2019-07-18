######################################################################
# Project name: Open source library to analyse pickup/dropoff locations of taxi trips

# Authors: Dr. Berk Anbaroglu
# Initial contributors: Caglayan Askin, Serdar Darendelioglu
# Date created: 2016 - 2018

# Purpose: Analyse the query performance of Postgres and MongoDB. But could be extended to other DBMS.

# Relevant paper is submitted to the Transportation journal.

# This library could be used for any purpose as long as the paper is cited.


######################################################################

# Import the necessary libraries
import random
import psycopg2
import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os


# MongoDB class
class mongoDB():
    def __init__(self,host, port, dbName):
        client = MongoClient(host, port)
        db = client.nyc
        self.collection = db[dbName]

        try:
            # The ismaster command is cheap and does not require auth.
            client.admin.command('ismaster')
            print("Connected to MongoDB Server\n\n")
        except ConnectionFailure:
            print("Mongodb Server not available\n\n")


# ----------------------- Queries related with the data quality    ---------------------------------------

#  --  How many trips have the same start and end time?

    def sameStartEndTime(self):
        # Retrives the query result: how many trips have same start and end time
        # Assumption: the trip start/end times are under the properties field:
        # i.e. properties.tpep_pickup_datetime, properties.tpep_dropoff_datetime
        query = {}
        query["$expr"] = {
            u"$eq": [
                u"$properties.tpep_pickup_datetime",
                u"$properties.tpep_dropoff_datetime"
            ]
        }


        start_time = datetime.datetime.now()
        cursor = self.collection.find(query)
        result = cursor.count()
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        del cursor
        return timediff, result

    def totalPrice_LTE2X(self, x):
        # Total price less than or equal to the input x.
        # x: total price in dollars
        # Assumption: the total amount field is here: properties.total_amount

        query = {}
        query["properties.total_amount"] = {
            u"$lte": x
        }

        start_time = datetime.datetime.now()
        cursor = self.collection.find(query)
        result = cursor.count()
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        del cursor
        return timediff, result

    def numPassengers_Equal2X(self, x):
        # Retrives the query result: How Many "passenger in the car" equal to x value
        # Assumption: the passenger count is here: properties.passenger_count

        query = {}
        query["properties.passenger_count"] = x

        start_time = datetime.datetime.now()
        cursor = self.collection.find(query)
        finish_time = datetime.datetime.now()
        result = cursor.count()
        timediff = (finish_time - start_time).total_seconds()

        del cursor
        return timediff, result

    def numLongTrips(self, threshold):
        # Retrieves the query result: How many trips took place greater than or equal to the given threshold in MILI SECONDS
        # 86,400,000 MILI Seconds = 1 Day

        pipeline = [
            {
                u"$project": {
                    u"properties": 1.0,
                    u"dateDifference": {
                        u"$subtract": [
                            u"$properties.tpep_dropoff_datetime",
                            u"$properties.tpep_pickup_datetime"
                        ]
                    }
                }
            },
            {
                u"$match": {
                    u"dateDifference": {
                        u"$gte": threshold
                    }
                }
            },
            {
                u"$count": u"passing_scores"
            }
        ]

        start_time = datetime.datetime.now()

        cursor = self.collection.aggregate(
            pipeline,
            allowDiskUse=True
        )

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        for doc in cursor:
            result = doc

        del cursor
        return timediff, result

    def find_MinMax_Pickup_Date(self):
        # This query is retrieving the maximum and minimum isodates of "Pick Up" Times.
        query = {}
        query["properties.tpep_pickup_datetime"] = {
            u"$exists": True
        }

        projection = {}
        projection["properties.tpep_pickup_datetime"] = 1.0

        sort = [(u"properties.tpep_pickup_datetime", 1)]

        cursor = self.collection.find(query, projection=projection, sort=sort).limit(1)
        min = cursor[0]['properties']['tpep_pickup_datetime']
        print("The Min Date in Database: ", min)

        query = {}
        query["properties.tpep_pickup_datetime"] = {
            u"$exists": True
        }

        projection = {}
        projection["properties.tpep_pickup_datetime"] = 1.0

        sort = [(u"properties.tpep_pickup_datetime", -1)]

        cursor = self.collection.find(query, projection=projection, sort=sort).limit(1)
        max = cursor[0]['properties']['tpep_pickup_datetime']

        print("The Max Date in Database: ", max)
        del query
        del cursor
        return min, max

# -------------------------  EOF: Queries related with the data quality    -----------------------------------

    def retrieveNumDocuments(self):
        return self.collection.count()

    def retrieveDocument(self, id):
        # Retrieves the trip having the the Postgres ID of 'id'

        query = {}
        query["properties.ID_Postgres"] = id
        # We need to project the output - all the fields must be set up
        projection = {}
        projection["geometry_pk.coordinates"] = 1.0
        projection["geometry_do.coordinates"] = 1.0
        projection["properties"] = 1.0


        cursor = self.collection.find(query, projection=projection)

        document = cursor[0]

        del cursor

        # To retrieve the coordinates of the pickup point:
        #print(doc['geometry_pk']['coordinates'][0])
        #print(doc['geometry_pk']['coordinates'][1])

        return document

    def retrieveDocument_singleDay(self, id):
        # We might also extract a single day, and need a document from there.
        # Retrieves the trip having the the NEW ID (nid) of 'id'
        # Assumption: The single day table has the field 'nid' starting from 1 to the number of trips on that day.


        query = {}
        query["properties.nid"] = id
        # We need to project the output - all the fields must be set up
        projection = {}
        projection["geometry_pk.coordinates"] = 1.0
        projection["geometry_do.coordinates"] = 1.0
        projection["properties"] = 1.0


        cursor = self.collection.find(query, projection=projection)

        document = cursor[0]

        del cursor

        # To retrieve the coordinates of the pickup point:
        #print(doc['geometry_pk']['coordinates'][0])
        #print(doc['geometry_pk']['coordinates'][1])

        return document

    # Spatial Query: point-in-polygon (pip) given the trip ID

    def pip_TripID(self,tripID):
        #This query retries the polygon in which the pickup of tripID resides
        #This query takes coordinates. And finding the polygons name which the point in inside.

        document = self.retrieveDocument(tripID)
        xP = document['geometry_pk']['coordinates'][0]
        yP = document['geometry_pk']['coordinates'][1]

        xD = document['geometry_do']['coordinates'][0]
        yD = document['geometry_do']['coordinates'][1]

        # Find the Origin Zone
        queryPickup = {}
        queryPickup["geometry"] = {
            u"$geoIntersects": {
                u"$geometry": {
                    u"type": u"Point",
                    u"coordinates": [
                        xP, yP
                    ]
                }
            }
        }

        # Find the Destination Zone
        queryDropoff = {}
        queryDropoff["geometry"] = {
            u"$geoIntersects": {
                u"$geometry": {
                    u"type": u"Point",
                    u"coordinates": [
                        xD, yD
                    ]
                }
            }
        }

        # Record the execution time of the query
        start_time = datetime.datetime.now()

        cursorPickup = self.collection.find(queryPickup)

        # It is possible for a point to be OUTSIDE of all zones
        # To handle that, we need to use a flag!
        flag = 0
        od = []
        for doc in cursorPickup:
            # print(x, ",", y, " -------------------> ", doc['properties']['zone'])
            # Zone name: doc['properties']['zone']
            flag = 1
            od.append(doc['properties']['LocationID'])
        if (flag == 0):
            od.append("None")

        cursorDropoff = self.collection.find(queryDropoff)

        for doc in cursorDropoff:
            flag = 1
            od.append(doc['properties']['LocationID'])
        if (flag == 0):
            od.append("None")

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()


        del cursorPickup, cursorDropoff

        return timediff, od

    # Spatial query: PIP - this time, instead of ID, we provide a time interval

    def pip_TimeInterval(self, interval):
        od = []
        start_time = datetime.datetime.now()

        # Find the coordinate list within the input interval
        projection = {}
        projection["geometry_pk.coordinates"] = 1.0
        projection["geometry_do.coordinates"] = 1.0
        # ORIGIN

        query = {}
        query["properties.tpep_pickup_datetime"] = {
            u"$gte": datetime.datetime.strptime(str(interval[0]), "%Y-%m-%d %H:%M:%S")
        }

        query["$and"] = [
            {
                "properties.tpep_pickup_datetime": {
                    u"$lt": datetime.datetime.strptime(str(interval[1]), "%Y-%m-%d %H:%M:%S")
                }
            }
        ]

        cursor = self.collection.find(query, projection=projection)

        # Obtain the coordinate list
        coordList_Pickup = []
        coordList_Dropoff = []
        for doc in cursor:
            # Pickup
            tmp = []

            tmp.append(doc['geometry_pk']['coordinates'][0])
            tmp.append(doc['geometry_pk']['coordinates'][1])
            coordList_Pickup.append(tmp)
            # Dropoff
            tmp = []
            tmp.append(doc['geometry_do']['coordinates'][0])
            tmp.append(doc['geometry_do']['coordinates'][1])
            coordList_Dropoff.append(tmp)

        # For each coordinate, find the polygon the point resides in
        totalPickupCoord = 0
        for coord in coordList_Pickup:
            totalPickupCoord += 1
            query = {}
            query["geometry"] = {
                u"$geoIntersects": {
                u"$geometry": {
                    u"type": u"Point",
                    u"coordinates": [
                        coord[0],
                        coord[1]
                                    ]
                               }
                                    }
                                }
            cursor = self.collection.find(query)

            # It is possible for a point to be OUTSIDE of all zones
            # To handle that, we need to use a flag!

            flag = 0
            for doc in cursor:
                flag = 1
                o = doc['properties']['LocationID']
            if (flag == 0):
                o = "None"

        totalDropoffCoord = 0
        for coord in coordList_Dropoff:
            totalDropoffCoord += 1
            query = {}
            query["geometry"] = {
                u"$geoIntersects": {
                u"$geometry": {
                    u"type": u"Point",
                    u"coordinates": [
                        coord[0],
                        coord[1]
                                    ]
                               }
                                    }
                                }
            cursor = self.collection.find(query)

            # It is possible for a point to be OUTSIDE of all zones
            # To handle that, we need to use a flag!!!
            flag = 0
            for doc in cursor:
                flag = 1
                d = doc['properties']['LocationID']
            if(flag == 0):
                d = "None"

            origin_destination = (o,d)
            od.append(origin_destination)


        #print("Total pickup: ", totalPickupCoord, "Total Dropoff", totalDropoffCoord)


        #print("O: ", O)
        #print("D: ", D)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        return timediff, od

    def pip_TimeInterval_v2(self, interval):
        start_time = datetime.datetime.now()
        od = []

        # Find the coordinate list within the input interval
        projection = {}
        projection["properties"] = 1.0

        # ORIGIN
        query = {}
        query["properties.tpep_pickup_datetime"] = {
            u"$gte": datetime.datetime.strptime(str(interval[0]), "%Y-%m-%d %H:%M:%S")
        }

        query["$and"] = [
            {
                "properties.tpep_pickup_datetime": {
                    u"$lt": datetime.datetime.strptime(str(interval[1]), "%Y-%m-%d %H:%M:%S")
                }
            }
        ]

        cursor = self.collection.find(query, projection=projection)

        for doc in cursor:
            # Pickup
            tripID = doc['properties']['ID_Postgres']
            duration_pip_tripID, tripID_od = self.pip_TripID(tripID)

            od.append(tripID_od)

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        return timediff, od

    # Spatial Query: k-NN ------------------------------------------

    def k_NN(self, tripID, k):
        # In order to find the k-NN of a tripID, we first need to find its coordinates; thus call the retrieveDocument method
        document = self.retrieveDocument(tripID)
        x = document['geometry_pk']['coordinates'][0]
        y = document['geometry_pk']['coordinates'][1]
        # print(x,y) - OK

        query = {}
        query["geometry_pk"] = {
            u"$nearSphere": {
                u"$geometry": {
                    u"type": u"Point",
                    u"coordinates": [
                        x, y
                    ]
                }
            }
        }

        # Record the execution time of the query
        start_time = datetime.datetime.now()
        cursor = self.collection.find(query).limit(k)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        k_NN = set()
        for doc in cursor:
            k_NN.add(doc['properties']['ID_Postgres'])

        del cursor
        return timediff, k_NN

    def k_NN_day(self, tripID, k):
        # k-NN anaylsis working on a single day

        document = self.retrieveDocument_singleDay(tripID)
        x = document['geometry_pk']['coordinates'][0]
        y = document['geometry_pk']['coordinates'][1]
        # print(x,y) - OK

        query = {}
        query["geometry_pk"] = {
            u"$nearSphere": {
                u"$geometry": {
                    u"type": u"Point",
                    u"coordinates": [
                        x, y
                    ]
                }
            }
        }

        # Record the execution time of the query
        start_time = datetime.datetime.now()
        cursor = self.collection.find(query).limit(k)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        k_NN = set()
        for doc in cursor:
            k_NN.add(doc['properties']['ID_Postgres'])

        del cursor
        return timediff, k_NN

#---------------------------      Update Functions     -----------------------------------------------------

    def update_sameStartEndTime(self):
        # Does the trip have the same start and end time?
        # If so it adds a flag.

        query = {}
        query["$expr"] = {u"$eq": [u"$properties.tpep_pickup_datetime", u"$properties.tpep_dropoff_datetime"]}

        start_time = datetime.datetime.now()

        update_query = self.collection.update_many(query, {u"$set": {"Errors.Flag_1": "true"}})

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        # Check:
        # print("Same start and end time update Worked. \n "
        #       " Modified:", update_query.modified_count, "\n "
        #         "It took", timediff, "seconds\n")

        # We can also do the reverse operations. That is remove the flag:

        # start_time = datetime.datetime.now()
        # antidote_update = self.collection.update_many(
        #     {"Errors.Flag_1": {u"$exists": 1}},
        #     {u"$unset": {"Errors.Flag_1": 1}})
        # finish_time = datetime.datetime.now()
        # timediff_antidote = (finish_time - start_time).total_seconds()
        #
        # print("sameStartEndTime Antidote Worked. \nModified: ", antidote_update.modified_count, "Antidote took: ", timediff_antidote, " seconds\n\n")

        return timediff, update_query.modified_count

    def update_totalPrice_LTE2X(self,x):
        # Below code adds a new field to those trips that have a "Total Price Less Than or Equal To X"

        query = {}
        query["properties.total_amount"] = {u"$lte": x}

        start_time = datetime.datetime.now()

        update_query = self.collection.update_many(query, {u"$set": {"Errors.Flag_2": "true"}})

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        # # Check:
        # print("totalPrice_LTE2X Update Worked. \nModified:", update_query.modified_count, "\nIt took", timediff,
        #       "seconds\n")


        # Below code is questioning if "Errors.Flag_2" attribute is exist.
        # If it is exist, it is deleting this attribute.

        # start_time_antidote = datetime.datetime.now()
        # antidote_update = self.collection.update_many(
        #     {"Errors.Flag_2": {u"$exists": 1}},
        #     {u"$unset": {"Errors.Flag_2": 1}}
        # )
        # finish_time_antidote = datetime.datetime.now()
        # timediff_antidote = (finish_time - start_time).total_seconds()
        #
        # print("totalPrice_LTE2X_update Antidote Worked. \nModified: ", antidote_update.modified_count, "Antidote Took: ",timediff_antidote," seconds\n\n")

        return timediff, update_query.modified_count

    def update_numPassengers_Equal2X(self,x):
        # Below code is searching for passenger_count value equal to x
        # As a result of search it is adding a new attribute named "Flag_3" under the Errors attribute.

        query = {}
        query["properties.passenger_count"] = x

        start_time = datetime.datetime.now()

        update_query = self.collection.update_many(query, {u"$set": {"Errors.Flag_3": "true"}})

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        # Check:
        # print("\nnumPassengers_Equal2X Update Worked. \nModified:", update_query.modified_count, "\nIt took", timediff,
        #       "seconds\n")


        # Below code is questioning if "Errors.Flag_3" attribute is exist.
        # If it is exist, it is deleting this attribute.

        # start_time = datetime.datetime.now()
        # antidote_update = self.collection.update_many(
        #     {"Errors.Flag_3": {u"$exists": 1}},
        #     {u"$unset": {"Errors.Flag_3": 1}}
        # )
        # finish_time = datetime.datetime.now()
        # timediff_antidote = (finish_time - start_time).total_seconds()
        #
        # print("totalPrice_LTE2X_update Antidote Worked.\nModified:", antidote_update.modified_count, "Antidote Took: ",timediff_antidote," seconds\n\n")

        return timediff, update_query.modified_count

    def update_longTrips(self, x):
        # Those trips that are longer than x MILISECONDS are flagged.

        pipeline = [
            {
                u"$project": {
                    u"properties": 1.0,
                    u"dateDifference": {
                        u"$subtract": [
                            u"$properties.tpep_dropoff_datetime",
                            u"$properties.tpep_pickup_datetime"
                        ]
                    }
                }
            },
            {
                u"$match": {
                    u"dateDifference": {
                        u"$gte": x
                    }
                }
            }
        ]

        start_time = datetime.datetime.now()

        cursor = self.collection.aggregate(pipeline, allowDiskUse=True)

        query = {}
        numLongTrips = 0
        for doc in cursor:
            query["properties.ID_Postgres"] = doc["properties"]["ID_Postgres"]
            self.collection.update(query, {u"$set": {"Errors.Flag_4": "true"}})
            numLongTrips = numLongTrips + 1

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        # Check:
        #print("\nnumLongTrips Update Worked. \nModified:", a, "\nIt took", timediff, "seconds\n")

        # Reverse operation:

        # antidote_update = self.collection.update_many(
        #     {"Errors.Flag_4": {u"$exists": 1}},
        #     {u"$unset": {"Errors.Flag_4": 1}})
        #

        return timediff, numLongTrips



    def update_sameStartEndLocation(self):
        # This function flags those trips whose pickup and dropoff coordinates are the same

        pipeline = [
            {
                u"$match": {
                    u"geometry_pk": {
                        u"$exists": True
                    },
                    u"geometry_do": {
                        u"$exists": True
                    },
                    u"properties.ID_Postgres": {
                        u"$exists": True
                    }
                }
            },
            {
                u"$project": {
                    u"geometry_pk": 1.0,
                    u"geometry_do": 1.0,
                    u"properties.ID_Postgres": 1.0,
                    u"AyniMi?": {
                        u"$eq": [
                            u"$geometry_pk.coordinates",
                            u"$geometry_do.coordinates"
                        ]
                    }
                }
            },
                   {
                       u"$match": {
                           u"AyniMi?": True
                       }
                   }
        ]

        start_time = datetime.datetime.now()

        cursor = self.collection.aggregate(pipeline, allowDiskUse=True)

        query = {}
        numSameLocation = 0
        for doc in cursor:
            query["properties.ID_Postgres"] = doc['properties']['ID_Postgres']

            self.collection.update_many(query, {u"$set": {"Errors.Flag_5": "true"}})
            numSameLocation = numSameLocation + 1

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        #print("\ndo_eq_pk_points Update Worked. \nModified:", numSameLocation, "\nIt took", timediff, "seconds\n")

        # Reverse operation:
        # antidote_update = self.collection.update_many(
        #     {"Errors.Flag_5": {u"$exists": 1}},
        #     {u"$unset": {"Errors.Flag_5": 1}})

        return timediff, numSameLocation


# -----------------------------------------------------------------------
#   -------------   Postgres

class postgres():
    # To improve the legibility of the queries, table names are not considered as an additional parameter.
    # Following table names are used: 
        # trips: table store all the trips
        # zones: table storing the TLC zones
    def __init__(self, dbName, userName, pswd, host, port):
        try:
            self.conn = psycopg2.connect(database=dbName,
                            user=userName,
                            password=pswd,
                            host=host,
                            port=port)
            print("Connected to PostgreSQL Server")
        except:
            print("Postgres connection failed!")

    def findMinMax_Interval(self, tableName, columnName):
        # At the moment this function returns the min-max of the column (could be id or nid - when a single day table is analysed) input
        cur = self.conn.cursor()

        query = "select min({}), max({}) " \
                "from {}".format(columnName, columnName, tableName)

        # Record the execution time of the query
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()

        cur.close()
        return timediff, result

    # --------------------------      Methods related with data import/export
    def postgres2GeoJSON(self, chunkSize, chunkID):
        # chunkSize: number of records to be converted to GeoJSON to ease the RAM operations
        # chunkID: the GeoJSON file is going to be saved by using this ID. starts from ZERO
        template = \
            '''
            {
            "type" : "Feature",
                "geometry_pk" : {
                    "type" : "Point",
                    "coordinates" : [%s,%s]},
                "properties": {
                    "ID_Postgres": %s,
                    "VendorID" : %s,
                    "passenger_count" : %s,
                    "store_and_fwd_flag" : "%s",
                    "RatecodeID" : %s,
                    "trip_distance" : %s,
                    "payment_type" : %s,
                    "fare_amount" : %s,
                    "extra" : %s,
                    "mta_tax" : %s,
                    "tip_amount" : %s,
                    "tolls_amount" : %s,
                    "improvement_surcharge" : %s,
                    "total_amount" : %s,
                    "tpep_pickup_datetime" : ISODate("%s"),
                    "tpep_dropoff_datetime" : ISODate("%s")},
                "geometry_do" : {
                    "type" : "Point",
                    "coordinates" : [%s,%s]},
            },

            '''

        # the head of the geojson file
        output = \
            '''
            '''

        outFileHandle = open("nyc2015_json_%s.geojson" % str(chunkID), "a")

        cur = self.conn.cursor()

        cur.execute(""" SELECT *
                            FROM staging
                            where id > {} and id <= {}
                            order by id """.format(chunkID * chunkSize, (chunkID + 1) * chunkSize))

        rows = cur.fetchall()
        c = 0
        for row in rows:
            record = ''
            id = row[0]
            vendorID = row[1]
            t_pickup = rearrangeTimeFormat(str(row[2]))
            t_dropoff = rearrangeTimeFormat(str(row[3]))
            passenger_count = row[4]
            trip_distance = row[5]
            pickup_longitude = row[6]
            pickup_latitude = row[7]
            ratecodeID = row[8]
            store_and_fwd_flag = row[9]
            dropoff_longitude = row[10]
            dropoff_latitude = row[11]
            payment_type = row[12]
            fare_amount = row[13]
            extra = row[14]
            mta_tax = row[15]
            tip_amount = row[16]
            tolls_amount = row[17]
            improvement_surcharge = row[18]
            total_amount = row[19]

            record += template % (pickup_longitude,
                                  pickup_latitude,
                                  id,
                                  vendorID,
                                  passenger_count,
                                  store_and_fwd_flag,
                                  ratecodeID,
                                  trip_distance,
                                  payment_type,
                                  fare_amount,
                                  extra,
                                  mta_tax,
                                  tip_amount,
                                  tolls_amount,
                                  improvement_surcharge,
                                  total_amount,
                                  t_pickup,
                                  t_dropoff,
                                  dropoff_longitude,
                                  dropoff_latitude)

            # Add the record to the GeoJSON file
            outFileHandle.write(record)

            c += 1

        # the tail of the geojson file
        output += \
                '''
                '''

        outFileHandle.write(output)
        outFileHandle.close()

        del rows
        cur.close()

    def postgres2GeoJSON_SubTable(self, chunkSize, chunkID, tableName):
        # chunkSize: number of records to be converted to GeoJSON to ease the RAM operations
        # chunkID: the GeoJSON file is going to be saved by using this ID. starts from ZERO
        # tableName: which table (usually something like: day_2015_22_08) is to be exported
        # this table has a new attribute, NEW ID (nid)!!!

        # Having a New ID (serial) is useful, since to find the min-max ID of a single day in
        # the trips table is a time consuming process!!!
        template = \
            '''
            {
            "type" : "Feature",
                "geometry_pk" : {
                    "type" : "Point",
                    "coordinates" : [%s,%s]},
                "properties": {
                    "nid": %s,
                    "ID_Postgres": %s,
                    "VendorID" : %s,
                    "passenger_count" : %s,
                    "store_and_fwd_flag" : "%s",
                    "RatecodeID" : %s,
                    "trip_distance" : %s,
                    "payment_type" : %s,
                    "fare_amount" : %s,
                    "extra" : %s,
                    "mta_tax" : %s,
                    "tip_amount" : %s,
                    "tolls_amount" : %s,
                    "improvement_surcharge" : %s,
                    "total_amount" : %s,
                    "tpep_pickup_datetime" : ISODate("%s"),
                    "tpep_dropoff_datetime" : ISODate("%s")},
                "geometry_do" : {
                    "type" : "Point",
                    "coordinates" : [%s,%s]},
            },

            '''

        # the head of the geojson file
        output = \
            '''
            '''

        outFileHandle = open("nyc2015_json_%s.geojson" % str(chunkID), "a")

        cur = self.conn.cursor()

        cur.execute(""" SELECT *
                              FROM {}
                              where nid > {} and nid <= {}
                              order by nid """.format(tableName, chunkID * chunkSize, (chunkID + 1) * chunkSize))

        rows = cur.fetchall()
        c = 0
        for row in rows:
            record = ''
            nid = row[0]
            id = row[1]
            vendorID = row[2]
            t_pickup = rearrangeTimeFormat(str(row[3]))
            t_dropoff = rearrangeTimeFormat(str(row[4]))
            passenger_count = row[5]
            trip_distance = row[6]
            pickup_longitude = row[7]
            pickup_latitude = row[8]
            ratecodeID = row[9]
            store_and_fwd_flag = row[10]
            dropoff_longitude = row[11]
            dropoff_latitude = row[12]
            payment_type = row[13]
            fare_amount = row[14]
            extra = row[15]
            mta_tax = row[16]
            tip_amount = row[17]
            tolls_amount = row[18]
            improvement_surcharge = row[19]
            total_amount = row[20]

            record += template % (pickup_longitude,
                                  pickup_latitude,
                                  nid,
                                  id,
                                  vendorID,
                                  passenger_count,
                                  store_and_fwd_flag,
                                  ratecodeID,
                                  trip_distance,
                                  payment_type,
                                  fare_amount,
                                  extra,
                                  mta_tax,
                                  tip_amount,
                                  tolls_amount,
                                  improvement_surcharge,
                                  total_amount,
                                  t_pickup,
                                  t_dropoff,
                                  dropoff_longitude,
                                  dropoff_latitude)

            # Add the record to the GeoJSON file
            outFileHandle.write(record)

            c += 1

        # the tail of the geojson file
        output += \
            '''
            '''

        outFileHandle.write(output)
        outFileHandle.close()

        del rows
        cur.close()


    # --------------------------      Queries related with the data quality
    # None Postgres IDs: [8M1 -10M]

    # -- How many trips start and end at the same location?
    def sameStartEndLocation(self):
        cur = self.conn.cursor()

        query = "select count(*) " \
                "from trips " \
                "where l_pickup = l_dropoff"

        # Record the execution time of the query
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()

        cur.close()
        return timediff, result


    #  --How many trips have the same start and end time?
    def sameStartEndTime(self):
        cur = self.conn.cursor()

        query = "select count(*) " \
                "from trips " \
                "where t_pickup = t_dropoff"

        # Record the execution time of the query
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()

        cur.close()
        return timediff, result


    # --How many trips have a total price LESS THAN or EQUAL to X?
    def totalPrice_LTE2X(self, x):
        # x: total price
        # Retrives the query result : How many "Total amount" value is less than or equal to input 'x'
        cur = self.conn.cursor()

        query = "select count(*) " \
                "from trips " \
                "where  total <= {}".format(x)

        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()
        cur.close()
        return timediff, result

    # --How many trips have X number of customers? Zero or less number of passengers are not meaningful
    def numPassengers_Equal2X(self, x):
        cur = self.conn.cursor()

        query = "select count(*) " \
                "from trips " \
                "where  num_passengers = {}".format(x)

        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()
        cur.close()
        return timediff, result

    # -- Some trips could be unrealistically long. Given a long definition in SECONDS
    def numLongTrips(self, threshold):
        cur = self.conn.cursor()

        query = "select count(*) "\
                "from trips " \
                "where DATE_PART('day', t_dropoff - t_pickup) * 60 * 60 * 24 + " \
                      "DATE_PART('hour', t_dropoff - t_pickup) * 60 * 60 + " \
                      "DATE_PART('minute', t_dropoff - t_pickup) * 60 + " \
                      "DATE_PART('second', t_dropoff - t_pickup) >= {}".format(threshold)

        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        result = cur.fetchall()
        cur.close()
        return timediff, result



    def k_NN_v1(self, tripID, k, nameIDColumn, tableName):
        # This query determines the k_NN of a a pickup location of a trip by joining the trip table twice
        # nameIDColumn: usually id, but could also be "nid" if a single day is analysed
        # tableName: which table are we relying on? trips (the whole table) or a specific day (day_yyyy_mm_dd)
        cur = self.conn.cursor()
        query = "SELECT y2.id " \
                "FROM {} y1, {} y2 " \
                "WHERE y1.{} = {} " \
                "ORDER BY y1.l_pickup <-> y2.l_pickup " \
                "limit {};".format(tableName, tableName, nameIDColumn, tripID, k)


        # Record the execution time of the query
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        # It is also important to have the NNs for comparison with other queries
        rows = cur.fetchall()
        k_NN = set()
        for row in rows:
            k_NN.add(row[0])


        cur.close()
        return timediff, k_NN



    def k_NN_v2(self, tripID,k, nameIDColumn, tableName):
        # This query determines the k_NN of a pickup location of a trip using id insertion
        # It has a similar idea to MongoDB query
        # nameIDColumn: usually id, but could also be "nid" if a single day is analysed
        # tableName: which table are we relying on? trips (the whole table) or a specific day (day_yyyy_mm_dd)

        cur = self.conn.cursor()
        query = "SELECT id " \
            "FROM {} " \
            "ORDER BY l_pickup <-> (select l_pickup from {} where {} = {})" \
            "limit {};".format(tableName, tableName, nameIDColumn, tripID, k)

        # We want to record the time of execution of the query
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        # It is also important to have the NNs for comparison with other queries
        rows = cur.fetchall()
        k_NN = set()
        for row in rows:
            k_NN.add(row[0])

        cur.close()
        return timediff, k_NN

    def pip_tripID(self, tripID):
        # pip: point_in_polygon
        # This method returns the Origin - Destination polygon of the pickup location of the trip ID
        cur = self.conn.cursor()

        q_pip = "SELECT z1.gid as O, z2.gid as D \n" \
                "FROM trips t \n" \
                "FULL JOIN zones z1 ON ST_Contains(z1.geom, t.l_pickup) \n" \
                "FULL JOIN zones z2 ON ST_Contains(z2.geom, t.l_dropoff) \n" \
                "WHERE t.id = {}".format(tripID)

        start_time = datetime.datetime.now()
        cur.execute(q_pip)
        finish_time = datetime.datetime.now()

        # Keep the OD of the trip
        od = cur.fetchall()

        cur.close

        return (finish_time - start_time).total_seconds(), od

    def pip_TimeInterval(self, interval, useCursor):
        # Interval is the random time interval the OD data is to be generated
        # useCursor is an optional parameter: it indeed speeds up the execution time considerably
        if(useCursor == True):
            cur = self.conn.cursor("with_cursor")
        else:
            cur = self.conn.cursor() # the traditional approach

        #print(interval[0], interval[1])

        # pip for the given time interval
        q_pip = "SELECT z1.gid as origin_zone, z2.gid as destination_zone \n" \
                "FROM trips t \n" \
                "FULL JOIN zones z1 ON ST_Contains(z1.geom, t.l_pickup) \n" \
                "FULL JOIN zones z2 ON ST_Contains(z2.geom, t.l_dropoff) \n" \
                "WHERE t.t_pickup >= '{}' and t.t_pickup < '{}'".format(interval[0], interval[1])    

        start_time = datetime.datetime.now()
        cur.execute(q_pip)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()

        od = cur.fetchall()
        cur.close

    
        # Note: If all the trip information is to be retrieved (i.e. taxiTrip.*) RAM error is observed:
        # Handling the "out of RAM error":
        # https://stackoverflow.com/questions/17199113/psycopg2-leaking-memory-after-large-query
        # http://initd.org/psycopg/docs/usage.html#server-side-cursors

        # Another error: "can't call .execute() on named cursors more than once"

        return timediff, od

    def pickup_pos(self,id):
        cur = self.conn.cursor()
        query = "SELECT l_pickup_lat,l_pickup_lon " \
                "FROM day_2015_05_23 " \
                "where nid= {}".format(id)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows

    def neighbor_pos(self,id):
        cur = self.conn.cursor()
        query = "SELECT l_pickup_lat,l_pickup_lon " \
                "FROM day_2015_05_23 " \
                "where id= {}".format(id)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows

    def pickup_pos_big(self,id):
        cur = self.conn.cursor()
        query = "SELECT l_pickup_lat,l_pickup_lon " \
                "FROM trips " \
                "where id= {}".format(id)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows

    def neighbor_pos_big(self,id):
        cur = self.conn.cursor()
        query = "SELECT l_pickup_lat,l_pickup_lon " \
                "FROM trips " \
                "where id= {}".format(id)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows


    # --------------------------------------    UPDATE Queries
    def addAttribute(self, attrName, type):
        # We can add a new attribute denoting the errors

        cur = self.conn.cursor()

        query = "alter table trips " \
                "add column \"{}\" {}".format(attrName, type)

        print(query)
        start_time = datetime.datetime.now()
        try:
            cur.execute(query)
        except:
            print("The attribute -", attrName, "- exists!")
        finish_time = datetime.datetime.now()
        # The update must be commited to be effective
        self.conn.commit()
        timediff = (finish_time - start_time).total_seconds()
        cur.close()

        return timediff

    def removeAttribute(self, attrName):
        cur = self.conn.cursor()

        # In case the attribute contains an upper case letter, the attrbute name should include the quotation marks

        query = "alter table trips " \
                "drop column \"{}\" ".format(attrName)

        start_time = datetime.datetime.now()
        try:
            cur.execute(query)
        except:
            print("The attribute ", attrName, " does not exist")
        finish_time = datetime.datetime.now()
        # The update must be commited to be effective
        self.conn.commit()
        timediff = (finish_time - start_time).total_seconds()
        cur.close()

        return timediff

    def addOD(self):
        start_time = datetime.datetime.now()
        cur = self.conn.cursor()

        t_addAttribute_originZone = self.addAttribute("origin_zone", "smallint")
        t_addAttribute_dropoffZone = self.addAttribute("dropoff_zone", "smallint")

        query = "UPDATE trips t "\
                "SET origin_zone = z1.gid, dropoff_zone = z2.gid "\
                "FROM zones z1, zones z2  "\
                "WHERE ST_CONTAINS(z1.geom, t.l_pickup) and ST_Contains(z2.geom, t.l_dropoff) and t.id<5"

        #t.id = 15623881 or t.id = 15623973 or t.id = 15624015

        # Somehow the update commit is NOT WORKING!


        cur.execute(query)

        self.conn.commit()

        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()
        cur.close()

        return timediff

    def addErrorTypes(self, attrName):
        cur = self.conn.cursor()
        self.removeAttribute(attrName)
        self.addAttribute(attrName, "character")

        # Find all the trips whose pickup and dropoff coordinates are the same:
        query = "update trips t " \
                "set {} = 1" \
                "where l_pickup = l_dropoff".format(attrName)

        # Record the execution time of the query
        start_time = datetime.datetime.now()
        cur.execute(query)
        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()


        self.conn.commit()

        cur.close()


    def extractDay(self, day):
        # E.g.: P.extractDay('2015_08_22')
        # Do not forget the underscore
        # The created table in Postgres would be: day_2015_08_22
        # GeoJSON files would besaved under the folder 'day_2015_08_22'
        # Chunk size is customized from the code.. Default: 100000
        # INDEX on pickup and dropoff location and time should be created manually!


        # nid is the new serial attribute inserted into the table!
        cur = self.conn.cursor()

        tableName = "day_" + day

        # Initialization: Remove the attribute - if it already exists
        query = "drop table {} ".format(tableName)

        try:
            cur.execute(query)
        except:
            print("The table ", tableName, " does not exist")

        # The update must be commited to be effective
        self.conn.commit()


        pkString = "pk_" + tableName

        # First create the table
        query = "CREATE TABLE {} " \
                "( " \
                "nid serial, " \
                "id integer NOT NULL," \
                "vendorid character varying(1)," \
                "t_pickup timestamp without time zone," \
                "t_dropoff timestamp without time zone," \
                "num_passengers smallint," \
                "trip_distance real," \
                "l_pickup_lon double precision," \
                "l_pickup_lat double precision," \
                "ratecodeid character(2)," \
                "flag_store character(1)," \
                "l_dropoff_lon double precision," \
                "l_dropoff_lat double precision," \
                "payment_type character(1)," \
                "fare_amount real," \
                "extra real," \
                "mta_tax real," \
                "surcharge real," \
                "tip real," \
                "tolls real," \
                "total real," \
                "l_pickup geometry(Point,4326)," \
                "l_dropoff geometry(Point,4326), " \
                "CONSTRAINT {} PRIMARY KEY (id)" \
                ")".format(tableName, pkString)

        cur.execute(query)

        self.conn.commit()

        # Define the new query to extract the day
        # Insert Into do not need 'VALUES' in this case!
        day2 = datetime.datetime.strptime(day, '%Y_%m_%d') + datetime.timedelta(days=1)
        #print("Next day: ", str(day2))

        query = "insert into {}(id, vendorid, t_pickup, t_dropoff, num_passengers, trip_distance, l_pickup_lon, l_pickup_lat, ratecodeid, flag_store, l_dropoff_lon, l_dropoff_lat, payment_type, fare_amount, extra, mta_tax, surcharge, tip, tolls, total, l_pickup, l_dropoff) " \
                "select * " \
                "from trips " \
                "where t_pickup >= '{}' and t_pickup < '{}'".format(tableName, day, day2)

        print(query)

        cur.execute(query)

        self.conn.commit()


        # Obtain the relevant GEOJSON's for MongoDB import
        try:
            os.makedirs(tableName)
        except OSError:
            pass
        # Change to that directory
        os.chdir(tableName)

        # Now run the postgres2GeoJSON to extract the necessary GeoJSON files
        chunkSize = 100000
        # Determine the number of chunks
        query = "select count(*)" \
                "from {} ".format(tableName)

        cur.execute(query)

        result = cur.fetchall()
        print("After fetchall: ", result)
        numTrips = result[0][0]
        print("Number of trips: ", numTrips)


        numChunks = (numTrips // chunkSize) + 1 # +1 because for e.g if we have 420K trips, we should have 5 GeoJSON file with a chunksize of 100K
        for chunkID in range(numChunks):
            self.postgres2GeoJSON_SubTable(chunkSize, chunkID, tableName)

        cur.close()


    def journeyTimeSeries(self, od, analysisInterval, timeInterval_Hour, timeInterval_Min, weekend):
        # This function would generate the time series of journey times for the given OD
        # at a given time interval e.g. timeInterval[0] = 9, timeInterval[1] = 10
        # for the analysis interval: datetime object (e.g. [datetime.date(2015,1,1), datetime.date(2015,1,31)])
        # If we are only interested in weekends, than weekend= True, otherwise False
        start_time = datetime.datetime.now()
        cur = self.conn.cursor()


        if(weekend):
            query = "SELECT id, (t_dropoff-t_pickup) \n" \
                    "FROM trips \n"\
                    "FULL JOIN zones z1 on st_contains(z1.geom, l_pickup) \n" \
                    "FULL JOIN zones z2 on st_contains(z2.geom, l_dropoff) \n" \
                    "WHERE t_pickup >= '{}' and t_pickup < '{}' \n" \
                    "AND (extract(hour from t_pickup) between {} and {}) \n" \
                    "AND (extract (minute from t_pickup) between {} and {}) \n" \
                    "AND z1.gid = {} and z2.gid = {} \n" \
                    "AND EXTRACT(DOW FROM t_pickup) in (0,6)".format(analysisInterval[0], analysisInterval[1], timeInterval_Hour[0], timeInterval_Hour[1], timeInterval_Min[0], timeInterval_Min[1], od[0], od[1])
        else:
            query = "SELECT id, (t_dropoff-t_pickup) \n" \
                    "FROM trips \n" \
                    "FULL JOIN zones z1 on st_contains(z1.geom, l_pickup) \n" \
                    "FULL JOIN zones z2 on st_contains(z2.geom, l_dropoff) \n" \
                    "WHERE t_pickup >= '{}' and t_pickup < '{}' \n" \
                    "AND (extract(hour from t_pickup) between {} and {}) \n" \
                    "AND (extract (minute from t_pickup) between {} and {}) \n" \
                    "AND z1.gid = {} and z2.gid = {} \n" \
                    "AND EXTRACT(DOW FROM t_pickup) in (1, 2, 3, 4, 5)".format(analysisInterval[0], analysisInterval[1],
                                                                     timeInterval_Hour[0], timeInterval_Hour[1],
                                                                     timeInterval_Min[0], timeInterval_Min[1], od[0],
                                                                     od[1])


        print(query)

        cur.execute(query)

        results = cur.fetchall()


        finish_time = datetime.datetime.now()
        timediff = (finish_time - start_time).total_seconds()
        cur.close()

        return timediff, results




        # --------------------------------------    Common Functions    --------------------------------------

def generateSQL2SelectIDs(IDs):
    # Input set of IDs
    # Generate a string corresponding to the SQL to select those IDs
    # Select the IDs from the main table of 'trips'
    numIDs = len(IDs)


    ids = ""
    for i in range(numIDs):
        tmp = "id = " + str(IDs[i]) + " or \n"
        ids = ids + tmp

    whereClause = "where " + ids

    strSQL = (""" SELECT *
                  FROM trips
                  {}""").format(whereClause)

    return strSQL



def generateRandomInterval(start, end, unit):
    # Generates a radom interval based on the arguments:
    # 1. Starting date
    # 2. End date
    # 3. Temporal unit in MINUTES up to a day (e.g. 5 min, 10 min, 30 min,... 1440 min): Returns the number of trips

    # start/end: datetime.date
    # Unit: in minutes

    # E.g. s = generateRandomInterval(datetime.date(2015,1,1), datetime.date(2015,1,31), 1440) # One day! OK
    # This qould generate a random day between 1 Jan 2015 and 31 Dec 2015
    

    startYear = start.year
    startMonth = start.month
    startDay = start.day

    endYear = end.year
    endMonth =  end.month
    endDay = end.day

    validDate = False

    # It is possible to randomly generate an invalid date (e.g. 31 April, 29 Feb 2017 etc.)
    # So make sure that the random date is a valid date!

    while not validDate:
        try:
            generatedYear = random.randint(startYear, endYear) # inclusive of the start/end year - no need to increment by 1
            generatedMonth = random.randint(startMonth, endMonth)
            generatedDay = random.randint(1, 31)
            # check if the generated day is a valid date
            date = datetime.date(generatedYear, generatedMonth, generatedDay)
        except:
            validDate = False
        else:
            validDate = True
            break

    # Randomly generate the time:
    tH = random.randint(0, 23)
    tM = random.randint(0, 59)
    tS = random.randint(0, 59)

    intervalStart = datetime.datetime(generatedYear, generatedMonth, generatedDay, tH, tM, tS)
    intervalEnd = intervalStart + datetime.timedelta(minutes=unit)

    return str(intervalStart), str(intervalEnd)


def generateRandomID_List(totalNumbers,maxID):
    # totalNumbers: how many random IDs are going to be generated?
    # maxID: max ID

    random_list = list()

    count = 0

    while(count < totalNumbers):
        id = random.randint(1, maxID)
        # Check whether the ID is indeed valid:
        # We have not included the Postgres IDs: [8M1 - 10M]
        if(id >= 8000001 and id <= 10000000):
            continue
        else:
            random_list.append(id)
            count += 1

    return random_list

# In order to have a legit temporal attribute, 'Z' must be added to the end of the date in MongoDB.
def rearrangeTimeFormat(t):
    # print(t)
    date = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    # print(str(date.isoformat()))
    # Adding the 'Z' at the end:
    s = str(date.isoformat())
    s = ''.join((s, 'Z'))
    # print(s)
    return s



