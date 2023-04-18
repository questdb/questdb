import sun.misc.BASE64Encoder;

public class TestDecoder {

  public static void main(String[] args) {
      System.out.println(new BASE64Encoder().encode(new byte[0]));
  }
}
