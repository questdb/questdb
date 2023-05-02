# Number of rows
r = 5

# Upper Triangles
for i in range(1, r+1):
    print("*"*i, end="")
    print(" "*(r-i)*2, end="")
    print("*"*i)

# Lower Triangles
for i in range(r,0,-1):
    print("*"*i, end="")
    print(" "*(r-i)*2, end="")
    print("*"*i)    
