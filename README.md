# powerflows


## Top Todos
Create get secrets, jobs  logs routes in backend rest api  XXX
delete render templates in backend rest api  
create frontend rest api with render templates XXX
call backend rest api for json webpages metadata and actions X
create docker container for frontend

nice to have : add nginx integration
nice to have: delegate execution of tasks in rest backend (celeri or docker/k8s)




### additional webpages
- display traces realtime
- create asset 
- change submit job to create job
- create submit job (from name)

### Metadata Asset

asset_id
name
asset_type
storage_connection_name
storage_location : variable by connection type (engine type) dont forget jobstorage
-> asset files [
mime_type
location_type
location_path]

### Jinja2 templating for querying language
use for querying capability of data asset

### Serializing formats
Serializer with CSV Json parquet orc arrow  parquet  with SQL capability
https://blog.jcharistech.com/2020/01/08/how-to-convert-json-to-sql-format-in-python/


### Connectivity capabilities: 
#### DB
executeQuery
BulkFile
OffloadToFile
BulkRest

##### PostGresql


#### COS
getObject
uploadObject
readDataSet
writeDataset
executeQuery

##### MinIO
##### IBM Cos
##### Azure Blob Storage
##### AWS S3
##### Generic S3


#### LOCALFS
downloadTable
updloadToTable
executeQuery


### Job Storage
#Job storage
use pandas for querying ? 
multi types
get storage with Serializer
ability to join multiple resultsets (pandas if in memory fs cos, or native db )
warning for high volume usage
-> for later
look for arrow for performance on non db joins ?

### Rest Capability : 
join JobStorage for input of parameters

# Graph object 
metadata that serves to orchestrate jobs

### Plugin system
#npm/ng style plugin addon capability:
- insert placeholders and templates in connexions.yaml
- insert placeholders python file in connections.engines

### Capabilities
#capability steps:
parrallel or sequential execution
leverage redis/celeri


### Internal datamodel
#create assets table job, connection, secrets etc
create common.utils function to load assets 
define asset structure in yaml file


### Job/Graph Run types
builtin
redis/celeri
containers

#ideas
use yield to stream queries from connection



# Tek todos
utils: 
- yaml/json indenter
test connection
discovery db assets 
discovery cos assets
discovery filesystem assets 
