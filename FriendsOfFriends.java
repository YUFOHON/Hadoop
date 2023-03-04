import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FriendsOfFriends {

  public static class FriendMapper
      extends Mapper<Object, Text, IntWritable, IntWritable>{

      private int mark_red = -1;
      public void map(Object key, Text value, Context context
		      ) throws IOException, InterruptedException {
	  StringTokenizer itr = new StringTokenizer(value.toString());
	  String one_string = itr.nextToken();
	  String two_string = itr.nextToken();
	  int one_integer = Integer.parseInt(one_string);
	  int two_integer = Integer.parseInt(two_string);

	  /* << your code here >> */
		  context.write(new IntWritable(one_integer), new IntWritable(two_integer));
		  context.write(new IntWritable(two_integer), new IntWritable(one_integer*mark_red));
      }
  }

  public static class FriendReducer
      extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

      private int mark_red = -1;

      public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
			 ) throws IOException, InterruptedException {
		  Vector<Integer> srcs = new Vector<Integer>();
		  Vector<Integer> dest = new Vector<Integer>();

	  /* << your code here >> */
		  for (IntWritable val:values) {
			  int id = val.get();
			  if (id <= mark_red) {
				  srcs.add(new Integer(id *mark_red));
			  } else {
				  dest.add(new Integer(id));
			  }
		  }
//		  combinate all to deastination a
		  for (int i = 0; i < srcs.size(); i++) {
			  for (int j = 0; j<  dest.size(); j++) {
				  context.write(new IntWritable(srcs.elementAt(i)), new IntWritable(dest.elementAt(j)));

			  }
		  }

      }
  }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length < 2) {
	    System.err.println("Usage: FriendsOfFriends <in> [<in>...] <out>");
	    System.exit(2);
	}
	Job job = Job.getInstance(conf, "Yu Fo Hon 20201702");
	job.setJarByClass(FriendsOfFriends.class);
	job.setMapperClass(FriendMapper.class);
	job.setReducerClass(FriendReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);
	for (int i = 0; i < otherArgs.length - 1; ++i) {
	    FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	}
	FileOutputFormat.setOutputPath(job,
				       new Path(otherArgs[otherArgs.length - 1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
