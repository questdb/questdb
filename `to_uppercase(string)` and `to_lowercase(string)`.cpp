#include <bits/stdc++.h>
using namespace std;
void to_uppercase(string &s)
{
	//using ASCII value
	int l = s.length(); 
       
    
    for (int i=0; i<l; i++) 
    {
    	if(s[i]>='a'&&s[i]<='z')
    	s[i] = s[i] - 32; 
    } 
	
}

void to_lowercase(string &s)
{
	//using ASCII value
	int l = s.length(); 
       
    
    for (int i=0; i<l; i++) 
    { 
    	if(s[i]>='A'&&s[i]<='Z')
        s[i] = s[i] + 32; 
    } 
	
}
