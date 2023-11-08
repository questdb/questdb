import java.util.*;
class Reverse 
{
    int s=0;
    Reverse(int n){
    while(n>0)
    {
        s=s*10+n%10;
        n/=10;
    }
    System.out.print(s);
    }
}
class Main 
{
    public static void main(String args[])
    {
        Reverse r=new Reverse(456);
    }
}
