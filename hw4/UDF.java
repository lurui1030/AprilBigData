package com.marlabs.bigdata;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/* input:  string 123-456-7890 or  string 123@gmail.com
output: text xxx-xxx-7890 or text xxx@gmail.com
*/

public class MaskingUDF extends UDF{
	private static String mask = "xxx";
	private static String dash = "-";
	public Text evaluate(String values) {
		String result = null;
		if(values.length() == 0) {
			return null;
		}
		String[] segments = values.split("[\\-@]");
		if(segments.length == 3) {
			result = mask + dash + mask + dash + segments[2];
		} else if(segments.length == 2) {
			result = mask + "@" +segments[1];
		} else {
			return null;
		}
		return new Text(result);
	}
}