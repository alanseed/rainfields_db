
import datetime
from rainfields_db import get_db, get_rainfields_df, get_states_df

start_time = datetime.datetime(year=2023,month=1,day=27,hour=0,tzinfo=datetime.UTC)
end_time = datetime.datetime(year=2023,month=1,day=27,hour=6, tzinfo=datetime.UTC) 
name = "AKL"
db = get_db() 
query = {
    "metadata.product":"QPE",
    "metadata.valid_time":{"$gte":start_time, "$lte":end_time}
}

# Read in the rain fields 
print("Reading the rain fields")
rainfields_df = get_rainfields_df(db,name,query)
for index,row in rainfields_df.iterrows():
    vtime = row["valid_time"].strftime("%Y-%m-%dT%H:%M:%S") 
    print(vtime) 

# Now read in the cascade states 
print("\nReading the state files")
states_df = get_states_df(db,name,query) 
for index,row in states_df.iterrows():
    vtime = row["valid_time"].strftime("%Y-%m-%dT%H:%M:%S") 
    print(vtime) 


