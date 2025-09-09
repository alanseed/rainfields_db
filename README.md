# rainfields_db  

A Mongo database manager for pySTEPS radar rainfal, state, and parameters.  

## Set up the MongoDB admin and user   

Need to create the admin user for the database and enable security

1. Set up the admin user 

```
mongosh
use admin
db.createUser({
  user: "myAdmin",
  pwd: "myPassword",
  roles: [ { role: "userAdminAnyDatabase", db: "admin" }, "readWriteAnyDatabase" ]
})
```
2. Set up the admin_credentials for rainfields_db  

Create a .rainfields_admin.env file in the $HOME directory 

```bash  
# Admin
ADMIN_USER=xxxx
ADMIN_PWD=xxxx
AUTH_DB=admin
MONGO_HOST=localhost
MONGO_PORT=27017  

```  

3. Install dependencies  

Install rainfields_db in the pysteps environment  

From the rainfields_db directory
pip install . 

Need to add the following in the environment
dotenv
pymongo
netCDF4 

4. Create the rainfields_db user

``` bash

python create_mongo_user.py --help 
usage: create_mongo_user.py [-h] [--db DB] [--role ROLE] [--password PASSWORD] username

Create a MongoDB user for a selected database.

positional arguments:
  username             Username to create

options:
  -h, --help           show this help message and exit
  --db DB              Target MongoDB database (default: rainfields_db)
  --role ROLE          MongoDB role (default: readWrite)
  --password PASSWORD  Optional password (otherwise generated)

```  

This generates a file with the credentials for the rainfields_db user in $HOME
```bash  
cat ~/.rainfields_user.env
DB_NAME=rainfields_db
DB_USER=radar
DB_PWD=radar
```

## Rain grid identification and format  

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
