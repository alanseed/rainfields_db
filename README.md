# rainfields_db  

Mongo database manager for pySTEPS radar rainfal, state, and parameters.  

The default name of the database is rainfields_db  

The rainfields_db is created by a database administrator who then assigns users to the database.  

The configuration for a set of domains, including the domain geometry and configurations  
for product generation, is managed in the config collection as a JSON file.  

Each domain is assigned a unque name e.g. "AKL" and the following collections are created:

* name.params - a collection of pySTEPS parameters and other field statistical properties  
* name.rain - A GridFS collection of rain fields stored as netCDF binaries  
* name.state - A GridFS collection of optical flow arrays and decomposed cascades  

Each rain field, state, parameter set is indexed using

* product - the product id  
* valid_time - the time of the rain field in UTC  
* base_time - the reference time of the forecast in UTC or None  
* ensemble - ensemble number or None

## scripts  

* create_mongo_user.py - A script to assign a user with authentication to the rainfields_db database  
* init_rainfields_db.py - A script to generate the collections and indexes for a specific domain name  

## io  

* gridfs_io.py - functions that manage reading and writing the binary netCDF and pySTEPS cascade data  
using the MongoDB GridFSBucket functions.  

## utils  

* db_utils.py - functions that are used to read and write the configuration and parameters from the  
database  
* nc_utils.py - functions to read and write netCDF files from the binary buffers  

## db  

Contains example files containing connection strings needed for the rainfields_db user. These files need to be  
placed in the home directory of the user and adapted for local database configuration etc.  
