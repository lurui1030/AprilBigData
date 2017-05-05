package invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class InvertedIndexReducer
  extends Reducer<Text, Text, Text, Text> {

  // remove duplicate
  // combine files
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    Set<Text> set = new HashSet<>();
    String output = "";
    for (Text fileName : values) {
      if (set.add(fileName)) {
        output += "|" + fileName.toString();
      }
    }
    context.write(key, new Text(output));
  }
}
