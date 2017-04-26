import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Created by yuyu on 4/25/17.
 */
public class NewFriend {
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            // ID1-ID2\tNumOfMutualFriends
            if(value == null) {
                return;
            }
            String source = value.toString().trim();
            if (source.length() == 0) {
                return;
            }
            String[] pairPlusNum = source.split("\\t");
            String[] ids = pairPlusNum[0].split("-");
            StringBuilder idPlusNum = new StringBuilder();
            idPlusNum.append(ids[1]).append(":").append(pairPlusNum[1]);
            context.write(new Text(ids[0]), new Text(idPlusNum.toString()));
        }
    }
    public static class OldFriendMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            if(value == null) {
                return;
            }
            String source = value.toString().trim();
            if (source.length() == 0) {
                return;
            }
            String[] userIdPlusFriends = source.split("\\s+");
            if (userIdPlusFriends.length < 2) {
                return;
            }
            String[] friends = userIdPlusFriends[1].split(",");
            for (int i = 0; i < friends.length; i++) {
                StringBuilder idPlusNum = new StringBuilder();
                idPlusNum.append(friends[i]).append(":").append(1);
                context.write(new Text(userIdPlusFriends[0]), new Text(idPlusNum.toString()));
            }
        }
    }
    public static class NewFriendReducer extends Reducer<Text, Text, Text, Text> {

        private int numOfNewFriends;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            numOfNewFriends = conf.getInt("numOfNewFriends", 5);
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
            for (Text value : values) {
                String[] idPlusNum = value.toString().split(":");
                if (hashMap.containsKey(idPlusNum[0])) {
                    hashMap.remove(idPlusNum[0]);
                }
                else {
                    hashMap.put(idPlusNum[0], Integer.valueOf(idPlusNum[1]));
                }
            }
            PriorityQueue<String> pq = new PriorityQueue<String>(numOfNewFriends, new Comparator<String>() {
                public int compare(String o1, String o2) {
                    String[] o1PlusNum = o1.split(":");
                    String[] o2PlusNum = o2.split(":");
                    return Integer.valueOf(o1PlusNum[1]) - Integer.valueOf(o2PlusNum[1]);
                }
            });
            int minNum = Integer.MAX_VALUE;
            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                if (!pq.isEmpty() && pq.size() == numOfNewFriends) {
                    if (entry.getValue() > minNum) {
                        pq.poll();
                    }
                    else {
                        continue;
                    }
                }
                minNum = Math.min(minNum, entry.getValue());
                StringBuilder idPlusNum = new StringBuilder();
                idPlusNum.append(entry.getKey()).append(":").append(entry.getValue());
                pq.add(idPlusNum.toString());
            }
            StringBuilder newFriends = new StringBuilder();
            for (String idPlusNum : pq) {
                String id = idPlusNum.split(":")[0];
                newFriends.append(id).append("|");
            }
            context.write(key, new Text(newFriends.toString()));
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("numOfNewFriends", args[3]);
        Job job = Job.getInstance(conf, "NewFriend");
        job.setJarByClass(NewFriend.class);
        job.setMapperClass(TransitionMapper.class);
        job.setMapperClass(OldFriendMapper.class);
        job.setReducerClass(NewFriendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OldFriendMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
