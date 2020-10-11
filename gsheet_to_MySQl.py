from re import error
import gspread
from mysql.connector import _CONNECTION_POOLS
import MySQLCredentials as mc
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials



# initializing variables for gspread
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('formdatatomysql-34eddc3281d3.json', scope)
client = gspread.authorize(creds)



# Function to pull data from spreadsheet
def GetSpreadsheetData(sheetName, worksheetIndex):
    sheet = client.open(sheetName).get_worksheet(worksheetIndex)
    return sheet.get_all_values()[1:]

data = GetSpreadsheetData('StudentList', 0)

print(data[0])
print(len(data))




# function to write to mysql db
def WriteToMySQLTable(sql_data, tableName):
    try:
# Connection credentials for MySQL.
       connection = mysql.connector.connect(
       user = mc.user,
       password = mc.password,
       host = mc.host,
       database = mc.database,
       auth_plugin = mc.auth_plugin
       )
       print(connection, "Successfully Connected!")

       sql_drop = " DROP TABLE IF EXISTS {tableName} ".format(tableName)


       sql_create_table = """CREATE TABLE {}(
           Full_Name VARCHAR(255),
           Class_Standing VARCHAR(20),
           Student_ID VARCHAR(16),
           Email VARCHAR(50),
           Photo VARCHAR(100),
           Major VARCHAR(100),
           Graduation_Date DATE,
           Credits_Completed VARCHAR(100),
           Advisor VARCHAR(30),
           PRIMARY KEY (Student_ID)
           )""".format(tableName)

       sql_insert_statement = """INSERT INTO {}(
           Full_Name,
           Class_Standing,
           Student_ID,
           Email,
           Photo,
           Major,
           Graduation_Date,
           Credits_Completed,
           Advisor )
           VALUES ( %s,%s,%s,%s,%s,%s, STR_TO_DATE(%s, '%d/%m/%Y') ,%s,%s )""".format(tableName)


       cursor = connection.cursor()
       cursor.execute(sql_drop)
       print('Table {tableName} has been dropped')
       cursor.execute(sql_create_table)


       print('Table {tableName} has been created')
# We need to write each row of data to the table, so we use a for loop
# that will insert each row of data one at a time
       print(sql_data)
       for i in sql_data:
           print(i)
           #print(sql_insert_statement)
           cursor.execute(sql_insert_statement, i)
# Now we execute the commit statement, and print to the console
# that the table was updated successfully
       connection.commit()
       print(f"Table {tableName} successfully updated.")
# Errors are handled in the except block, and we will get
# the information printed to the console if there is an error
    except mysql.connector.Error as error:
        print(f"Error: {error}. Table {tableName} not updated!")
        # connection.rollback()
        


# We need to close the cursor and the connection,
# and this needs to be done regardless of what happened above.



WriteToMySQLTable(data, 'StudentList')

# Replaces any empty cells with 'NULL'
def preserveNULLValues(listName):
   print('Preserving NULL values...')
   for x in range(len(listName)):
       for y in range(len(listName[x])):
           if listName[x][y] == '':
               listName[x][y] = None
   print('NULL values preserved.')

# Uses Google Drive's API.
# If you get an error regarding this, go to the link and enable it.


# Write to the table in the database.
preserveNULLValues(data)
