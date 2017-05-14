package hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

public class EncryptEmail
    extends UDF {
  public Text evaluate(Text email) {
    try {
      int lastAt = email.toString().lastIndexOf("@");
      char[] en = new char[lastAt];
      Arrays.fill(en, 'X');
      return new Text(new String(en) + email.toString().substring(lastAt));
    } catch (Exception e) {
      return new Text();
    }
  }
}
