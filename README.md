# rainfields_db  

A Mongo database manager for pySTEPS radar rainfal, state, and parameters.  

## Rain fall grid identification and format  

A rainfall grid is identified by:

* product  
* domain  
* valid time  
* forecast base time  
* ensemble number  

The rainfall grids are stored using the MongoDB GridFS utility. Each file is a netCDF CF compliant binary  
with rainfall saved as 16-bit integer values of rainfall intensity with 0.1 mm/h resolution with the metadata  
stored alongside the netCDF file as a separate document.  

Each file requires a unique name, which is generated using the make_nc_name function.
The file naming convention can be configured as part of the product specification, but the default structure is:  
"$D_$P_$V{%Y-%m-%dT%H:%M:%S}_$B{%Y-%m-%dT%H:%M:%S}_$E.nc" where  

* $D is the domain ID  
* $P is the product ID  
* $V is the valid time of the grid in UTC  
* $B is the forecast base time in UTC if the product is a forecast  
* $E is the ensemble number if this product is an ensemble forecast  

## Data base structure  

The database consists of a set of collections:  

* domain - The location, size and resolution of the grids for each domain
* config - Configuration for each product, both input and pySTEPS output  
* params - A collection with the pySTEPS parameters for each input product  
* stats - A collection with the field statistics for each product  
* rain - A MongoDB GridFS collection of rain fields that are stored as netCDF binaries  
* state - A MongoDB GridFS collection of rainfield cascades and the Optical Flow advection fields  

## Database adinistration  

The system admiministrator initialies a rainfields_db database and allocates users to it.  

The envoronment variables for the data base are managed by two files in each user's home directory:  

.rainfields_admin.env  
Manages the username and password of the system administrator for the MongoDB system, and the location of the database (which could also be a MongoAtlas instance)  

```bash  
# Admin
ADMIN_USER=xxxx
ADMIN_PWD=xxxx
AUTH_DB=admin
MONGO_HOST=localhost
MONGO_PORT=27017  

```  

.rainfields_user.env  
Manages the name of the database and the credentials needed for read/write access to that specific database.  

```bash  
DB_USER=xxxx  
DB_PWD=xxxx
DB_NAME=rainfields_db  
```

The rainfields_db is initialized once the environment fields are setup using the  
scripts in the rainfields_db/scripts directory.  

* init_rainfields_db.py -  
    Initialize a database with the expected collections and indexes  
* create_mongo_user.py -  
    A script to assign a user with authentication to a database  

## io  

* gridfs_io.py -  
    Functions that manage reading and writing the binary netCDF and pySTEPS cascade data using 
    the MongoDB GridFSBucket functions.  
* params_io.py -  
    Functions that manage reading and writing the parameter documents  
* stats_io.py -  
    Functions that manage reading and writing the field statistics doduments  

## utils  

* db_utils.py -  
    Functions that are used to connect to the correct database and read and write a configuration  
    document.  
* nc_utils.py -  
    Functions to read and write netCDF files from the GridFS "rain" collections.  
