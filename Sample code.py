//To find the id of questDB client and server if the id is greater than or equal to 5000 print client and less than or equal to 5000 print server.
//This code is based on sample code written by me and not associated with questdb for trying purpose only.

a=int(input())
if(a>=5000):
  print("Quest Db is connected to client")
else if(a<=5000):
  print("Quest Db is connected to server")
else:
  print("Disconnected")
