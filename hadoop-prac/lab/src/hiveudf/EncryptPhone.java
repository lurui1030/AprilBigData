package hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class EncryptPhone
    extends UDF {
  public Text evaluate(Text phoneNumber) {
    try {
      String[] sections = phoneNumber.toString().split("-");
      return new Text("XXX-XXX-" + sections[2].trim());
    } catch (Exception e) {
      return new Text();
    }
  }
}
