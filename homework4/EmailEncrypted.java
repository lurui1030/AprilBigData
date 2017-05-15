package com.marlabs.bigdata.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class EmailEncrypted extends UDF{
	public Text evaluate(Text column){
		String s = column.toString();
		String cleaned_column = "XXX" + s.substring(s.indexOf("@"));
		return new Text(cleaned_column);
	}
}
