"""
- sample API-request for performance measuring
- write response into a Postgres-Database
- makes use of Multithreading to enhance performance
- gets areaUnits from ws/valuations/{id}?expand=areaUnitList
- use maxNoOfEntities to define max number of entities to be retreived
"""

import psycopg2
from psycopg2.extras import Json
from configparser import ConfigParser
import os
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
import json

#gets the enviromment variables from .env-file, if there is one
load_dotenv()


#reads the database connection from the specified filename
def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

#implements an insertion into the postGres database
#inserts the mayload into a table "json_ruby"
#if the table doesn't exist it is being created first
def insert_json(dict_id, dict_obj, modDate):
    """ Insert a new JSON Object into the Repository table """
    sql_create = """CREATE TABLE IF NOT EXISTS public.json_ruby(
                json_id VARCHAR(50) UNIQUE,
                json_col JSON,
                json_ruby_pk SERIAL4,
                mod_date TIMESTAMP,
                PRIMARY KEY(json_ruby_pk)  
            );"""

    sql_insert = """INSERT INTO public.json_ruby AS old (json_id, json_col, mod_date)
             VALUES(%s, %s, %s)
             ON CONFLICT (json_id) DO UPDATE SET
             json_col = EXCLUDED.json_col,
             mod_date = EXCLUDED.mod_date
             WHERE
              EXCLUDED.mod_date > old.mod_date
             RETURNING json_id;"""

    #should not be used, column names not correct
    # sql_query = """DROP TABLE IF EXISTS public.Mietobjekte;
    #             SELECT
    #                 json_col ->> 'id' as BewId,
    #                 json_col ->> 'method' as BewType,
    #                 json_col ->> 'isMaster' as Master,
    #                 json_col ->> 'status' as BewStatus,
    #                 CAST((json_col -> 'keyFigures' ->> 'ownMarketValue') AS DECIMAL) as Marktwert,
    #                 json_array_elements(json_array_elements(json_col -> 'embedded') -> 'value' -> 'areaUnits') ->> 'id' as MietobjektId,
    #                 CAST(json_array_elements(json_array_elements(json_array_elements(json_col -> 'embedded') -> 'value' -> 'areaUnits') -> 'leases') ->> 'calculatedStart' AS DATE) as LeaseStart,
    #                 CAST(json_array_elements(json_array_elements(json_array_elements(json_col -> 'embedded') -> 'value' -> 'areaUnits') -> 'leases') ->> 'expectedEnd' AS DATE) as LeaseEnd,
    #                 json_array_elements(json_array_elements(json_col -> 'embedded') -> 'value' -> 'areaUnits') ->> 'units' as Anzahl,
    #                 json_array_elements(json_array_elements(json_col -> 'embedded') -> 'value' -> 'areaUnits') ->> 'isVacantAtValuationDate' as Leerstand,
    #                 CAST(json_array_elements(json_array_elements(json_array_elements(json_col -> 'embedded') -> 'value' -> 'areaUnits') -> 'leases') ->> 'currentIncome' AS DECIMAL) as IstMiete,
    #                 json_array_elements(json_array_elements(json_array_elements(json_col -> 'embedded') -> 'value' -> 'areaUnits') -> 'leases') ->> 'tenant' as Mieter
    #             INTO
    #                 public.Mietobjekte
    #             FROM
    #                 public.json_ruby;"""


    conn = None
    vendor_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the CREATE TABLE statement
        cur.execute(sql_create)
        # execute the INSERT statement
        cur.execute(sql_insert, (dict_id, Json(dict_obj), modDate))
        # execute the SELECT statement
        #cur.execute(sql_query)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return dict_id


def insert_sql(payload):
    for dict_index, dict_content in enumerate(payload):

        #tmstp_dict_content = datetime.strptime(dict_content['modificationDate'], '%Y-%m-%dT%H:%M:%S.%f')

        #dummy timestamp, da auf /ivm-rental-contracts kein modificationDate existiert
        tmstp_dict_content = datetime.now()

        dict_key = dict_content['id']
        insert_json(dict_key, dict_content, tmstp_dict_content)


def get_request(URL, header):
    results = None

    try:
        response = requests.get(URL, headers=header)
        results = json.loads(response.text)

    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        print("Timeout. Try again.")
    except requests.exceptions.TooManyRedirects:
        # Tell the user their URL was bad and try a different one
        print('Bad URL: {}'.format(URL))
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        raise SystemExit(e)

    finally:
        return results


def query_api():
    start_time = time.time()

    # Get environment variables
    try:
        authToken = os.environ['auth_token']
        baseURL = os.environ['BaseUrl']
        noOfEntities = int(os.environ['maxNoOfEntries'])
    except KeyError as e:
        print("Key error occured when trying to retreive environment variables")
        raise SystemExit(e)

    # header for API-request
    header = {'Authorization': 'Bearer ' + authToken}

    # check Validity of environment variables
    # should be at least one and is capped to 500
    assert noOfEntities in range(1, 11), "Number of entities out of range (1-10)"

    # the combination of BaseURL + authToken should give a valid webservice response (status code 200)
    statusCode = requests.get(baseURL + '/ping', headers=header).status_code

    print("Status Code ping request:")
    print(statusCode)

    assert statusCode == 200, "Webservice not reachable with the provided BaseURL and authentication token"

    # Get number of pages from the indexer-response of propertyUnits (set size to max allowed number for max. efficiency)
    URL = baseURL + '/index/property-units?size=100'

    results = get_request(URL, header)

    nPages = results.get('page', {}).get('totalPages', {})
    nElements = results.get('page', {}).get('totalElements', {})

    print('Anzahl Seiten: ' + str(nPages))
    print('Anzahl PropertyUnits: ' + str(nElements))

    # Liste zum Abspeichern der Property-Units
    propertyUnits = []

    #Get all property units per page
    #for i in range(1):
    for i in range(nPages):

        URL = baseURL + '/index/property-units?size=100&page=' + str(i)

        results = get_request(URL, header)

        print('Get results from page #: {}'.format(str(i)))

        if results: propertyUnits = propertyUnits + results.get('content', {})

    #Will contain the JSON-requests from the rental contracts endpoint
    payload = []

    #First get all rental-contract links for every property unit (result will be a list)
    #Then get all data for all rental contracts
    for index, pu in enumerate(propertyUnits):

        #do not exceed max. number of property units to be loaded
        if index < noOfEntities:

            id = pu.get('id', {})

            print('Get list of Rental Contracts for PropertyUnit#: {}'.format(str(index)))
            print('Get list of Rental Contracts for PropertyUnit: {}'.format(id))

            full_url = baseURL + '/property-units/' + id + '/ivm-rental-contracts'
            results = get_request(full_url, header)

            #list of all rental contracts for a given property unit
            rentalContracts = results.get('content',{})

            noOfRentalContracts = len(rentalContracts)

            print('# of RentalContracts found: {}'.format(noOfRentalContracts))

            #get all data from all rental contracts
            for index, rc in enumerate(rentalContracts):


                href = rc.get('links',{})[0].get('href',{})

                print('Get Rental Contract on : {}'.format(href))

                results = get_request(href, header)

                payload.append(results)

        else:

            break

    # Inject Jsons to SQL
    insert_sql(payload)

    end_time = time.time()
    time_elapsed = (end_time - start_time)
    print('Total Runtime API-Call: {} seconds'.format(round(time_elapsed)))
    return payload

if __name__ == '__main__':
    query_api()