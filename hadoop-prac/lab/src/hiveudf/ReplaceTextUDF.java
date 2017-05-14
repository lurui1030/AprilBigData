package hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ReplaceTextUDF extends UDF {
  public Text evaluate(String value, String oldStr, String newStr) {
    return new Text(value.replace(oldStr, newStr));
  }
}
