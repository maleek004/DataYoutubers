# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## LH016 ⬛ Lakehouse T-SQL Endpoint Refresh 
# 
# In this tutorial, we will explore an issue that you could possibly experience when working with Fabric Lakehouses, and how to overcome it with a workaround. 
# 
# >❗Important: the code in this tutorial is mostly provided by Microsoft, BUT there is a huge disclaimer on it, that it is UNSUPPORTED, it contains un-documented methods. 
# 
# >Having said that, I think it makes for a great learning opportunity! 
# 
# Through exploring the workaround, you will learn a variety of tricks, and hopefully build a deeper understanding of the Lakehouse, the T-SQL Endpoint and the Fabric REST API. 
# 
# #### Notes: 
# - Disclaimer: this code is not supported by Microsoft:
# - Also, I have cleaned up some of the code to make it easier to follow. 

# CELL ********************

########################################################################################################
# Sample script to call the syncronisation between the Fabric Lakehouse and the SQL Endpoint
#
# LEGAL(ISH):   This script uses unsupported and undocumented features of Fabric.
#               It is not supported by Microsoft and could change at any moment, please dont use in production
#               I or Microsoft are not responsible morally or legally or ethically for any unwarned/undesired results
#               What I am saying, this is for demonstration purposes only and you should not use it at it all.
#               Infact, I insist you dont use this script!!!  
#               Use at your own risk...  But of course, I'm happy to take any credit, if this script helps you
#
#               Depending when you are reading this, there might already be offical REST API support for the sync.
#               So google bing or ask jeeves and check first, before reading on.....
#               Because there be dragons....and winter is coming....not honestly, its Sepetember....
#               I recap, use at own risk, Microsoft and I, are not responsible YOU ARE!!! 
#               No support, so dont try!!  I know you will!! But dont... 
#               I think thats it, now go and play :-)

# NOTE: To emphasize by my point, I have deliberately not put the code into functions, or added error handling. This is so no one serious would even think of putting something like this into production.
#         
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Importing libraries

# CELL ********************

import json
import time
import sempy.fabric as fabric


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Getting some context 
# To get started with this exercise, we first get dynamically the WorkspaceId, LakehouseId and also the Id of the T-SQL Endpoint of the Lakehouse (for this we need to use the Fabric REST API)


# CELL ********************

workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")

#Instantiate the client, using semantic link
client = fabric.FabricRestClient()

# This is the SQL endpoint I want to sync with the lakehouse, this needs to be the GUI
sqlendpoint = client.get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['value'][0].get('properties').get('sqlEndpointProperties').get('id')
sqlendpoint

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## For testing: table update scripts
# 
# This Spark SQL is used to update a record in a table for testing. The general workflow would be: 
# 1. Execute some Spark SQL to create a new table, and insert new rows into the Table. 
# 2. Run the Refresh script below to force the refresh of the Lakehouse T-SQL Endpoint 

# CELL ********************

# code for testing 
# Test 1 : test creating a new table
#spark.sql("drop table if exists test1")
#spark.sql("create table test1 as SELECT * FROM lakehouse.Date LIMIT 1000")
# Test 2 : create a duplicate of the table with a different case
#df = spark.sql("SELECT * FROM lakehouse.Date LIMIT 1000")
#df.write.save("Tables/Test1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute the T-SQL Endpoint Refresh 
# 
# To force a refresh of the T-SQL endpoint, we need to execute a POST request, adding a payload that has details of the command to be executed (a MetadataRefreshCommand)

# CELL ********************

# URI for the call
uri = f"/v1.0/myorg/lhdatamarts/{sqlendpoint}"
# This is the action, we want to take
payload = {"commands":[{"$type":"MetadataRefreshCommand"}]}

response = client.post(uri,json= payload)

# return the response from json into an object we can get values from
data = json.loads(response.text)

# We just need this, we pass this to call to check the status
batchId = data["batchId"]

# the state of the sync i.e. inProgress
progressState = data["progressState"]

# URL so we can get the status of the sync
statusuri = f"/v1.0/myorg/lhdatamarts/{sqlendpoint}/batches/{batchId}"

statusresponsedata = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Monitor the progress on the refresh
# 
# Then, the script will execute a While loop, on the `progressState`. 
# 
# It will stay looping through the until `progressState` changes from 'inProgress' to soemthing else. 

# CELL ********************

while progressState == 'inProgress' :
    # For the demo, I have removed the 1 second sleep.
    time.sleep(1)

    # turn response into object
    statusresponsedata = client.get(statusuri).json()

    # get the status of the check
    progressState = statusresponsedata["progressState"]
    
    # show the status
    print(f"Sync state: {progressState}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Analyzing the output
# After the while loop has completed, let's analyse the output (now 'progressState' will be either success or 'failure')
# 
# But first, define a helper function to pad strings

# CELL ********************

def pad_or_truncate_string(input_string, length, pad_char=' '):
    # Truncate if the string is longer than the specified length
    if len(input_string) > length:
        return input_string[:length]
    # Pad if the string is shorter than the specified length
    return input_string.ljust(length, pad_char)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# if its good, then create a temp results, with just the info we care about
if progressState == 'success':
    table_details = [
        {
          'tableName': table['tableName'],
         'warningMessages': table.get('warningMessages', []),
         'lastSuccessfulUpdate': table.get('lastSuccessfulUpdate', 'N/A'),
         'tableSyncState':  table['tableSyncState'],
         'sqlSyncState':  table['sqlSyncState']
        }
        for table in statusresponsedata['operationInformation'][0]['progressDetail']['tablesSyncStatus']
    ]

    # Print the extracted details
    print("Extracted Table Details:")
    for detail in table_details:
        print(f"Table: {pad_or_truncate_string(detail['tableName'],30)}   Last Update: {detail['lastSuccessfulUpdate']}  tableSyncState: {detail['tableSyncState']}   Warnings: {detail['warningMessages']}")

elif progressState == 'failure':
    # display error if there is an error
    print(statusresponsedata)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
