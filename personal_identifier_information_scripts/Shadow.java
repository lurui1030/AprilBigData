package com.marlabs.bigdata.PII;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * The user defined function that shadow the email: abcd@gmail.com -> ***@gmail.com
 * @author yuan
 *
 */

@Description(
		name = "Shadow",
		value = "_FUNC(str) - replace the values before @ to *** for email address, replace the phone number to ***-***-(last four digits)",
		extended = "Example:\n" + 
		" > SELECT firstName,shadow(str) from personal_Identifier_Information" + 
		" ***@gmail.com / ***-***-2580"
		)

public class Shadow extends UDF{
	private Text result = new Text();
	
	public Text evaluate(String str){
		if(str.length()!=0){
			if(str.contains("@")){
				int counter = 0;
				while(str.charAt(counter) !='@'){
					counter++;
				}
				result.set("***" + str.substring(counter, str.length()));
			}
			else{
				result.set("***-***-" + str.substring(str.length()-4,str.length()));
			}
			return result;
		}		
		return null;
	}
}
