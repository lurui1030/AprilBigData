package com.marlabs.bigdata.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class PhoneNumberEncrypted extends UDF{
	public Text evaluate(Text column){
		String cleaned_column = column.toString().substring(0, 7).replaceAll("[0-9]", "X") + column.toString().substring(7);
		return new Text(cleaned_column);
	}
}
