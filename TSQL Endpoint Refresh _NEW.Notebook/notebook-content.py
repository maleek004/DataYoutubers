# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5c8c217e-16f4-4499-b7c3-244fd38f8694",
# META       "default_lakehouse_name": "youtubers_LH",
# META       "default_lakehouse_workspace_id": "ec80c81a-c58e-4508-817c-17f6c253f83a",
# META       "known_lakehouses": [
# META         {
# META           "id": "5c8c217e-16f4-4499-b7c3-244fd38f8694"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json 
import notebookutils 
import sempy.fabric as fabric 
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException 

def pad_or_truncate_string(input_string, length, pad_char=' '):
    # Truncate if the string is longer than the specified length
    if len(input_string) > length:
        return input_string[:length]
    # Pad if the string is shorter than the specified length
    return input_string.ljust(length, pad_char)

def display_return(data):
    table_details = [
        {
        'tableName': table['tableName'],
        'status':  table['status'],
        'startDateTime':  table['startDateTime'],
        'endDateTime':  table['endDateTime'],
        'lastSuccessfulSyncDateTime':  table['lastSuccessfulSyncDateTime'],
        'error':  table['error']
        }
        for table in data
        ]
    for detail in table_details:
        print(f"Table: {pad_or_truncate_string(detail['tableName'],20)} status: {detail['status']} start: {detail['startDateTime']}  end: {detail['endDateTime']}  Last Update: {detail['lastSuccessfulSyncDateTime']}  error: {detail['error']}  ")
    print('')
    #print(data)
    
    return;


workspace_id=spark.conf.get("trident.workspace.id")
#lakehouse_id = notebookutils.lakehouse.getWithProperties(name='youtubers_LH', workspaceId=workspace_id)['id']
lakehouse_id=spark.conf.get("trident.lakehouse.id")

#Instantiate the client
client = fabric.FabricRestClient()

# This is the SQL endpoint I want to sync with the lakehouse, this needs to be the GUI
sqlendpoint = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['id']

# URI for the call 
uri = f"v1/workspaces/{workspace_id}/sqlEndpoints/{sqlendpoint}/refreshMetadata?preview=true" 

# This is the action, we want to take 
payload = {} 

try:
    response = client.post(uri,json= payload, lro_wait = True) 
    sync_status = json.loads(response.text)
    display_return(sync_status)

except Exception as e: print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# use an actual lakehouse id for getting the SQL endpoint
#  

# CELL ********************

fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# * Your notebook has to be connected to the lakehouse that you want to refresh for this code to work correctly 
# * if its not connected to a default lakehouse, you will get a key error when trying to call the fabric API to assign a value to the SQL endpoint variable. To avoid this just assign the lakehouse that you want to refresh ad default lakehouse for this notebook. But to kind of fix it...:
# * The error is because in the previous line, `spark.conf.get("trident.lakehouse.id")` will return an empty string which will now be your lakehouse_id variable. Then you are not specifying a lakehouse id when trying to get the SQL endpoint variable. your code will look like this: `fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/")`
# * the json response from the API when you don not specify a lakehouse id will be a little different, it will return a list containing  'lakehouse_description_dictionries' for every lakehouse in your workspace. so trying to get a 'properties' key directly from the response will fail because the property you are looking for is now within a dictionary, in a list, in another dictionary (while the original code assumes the response is a single dictionary) 
# * If you have only one lakehouse in your workspace, then modify the line for getting its sql endpoint to: `sqlendpoint = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['value'][0]['properties']['sqlEndpointProperties']['id']` 
# * but if you have multiple lakehouses in your workspace , you need to know what the index of your target lakehouse will be (i think it is ordered by creation date) and replace [0] with it


# CELL ********************

fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/").json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

sqlendpoint

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

lakehouse_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# '5c8c217e-16f4-4499-b7c3-244fd38f8694'

