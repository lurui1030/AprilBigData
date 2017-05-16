package hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class EncryptEmail
    extends UDF {
  public Text evaluate(Text email) {
    try {
      int lastAt = email.toString().lastIndexOf("@");
      return new Text("XXX" + email.toString().substring(lastAt));
    } catch (Exception e) {
      return new Text();
    }
  }
}
