# APItoSQLTest
This python-script gets all data for the rental contracts endpoint ({BaseURL}/property-units/{pu.id}/ivm-rental-contracts/{id}) and stores it into a PostGres Database
It serves as a load-test to measure performance
The whole JSON-response from each rental contract will be stored in one line in the database 
The variables for the baseurl, access token and number of propertyUnits to be loaded have to be accessed from an .env file
The runtime of the script (number of property units to load rental contracts from) can be calibrated by the variable maxNoOfEntries
The configuration for the PostGres is loaded from database.ini -> this file has to be adapded to your database
