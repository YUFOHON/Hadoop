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

public class A1 {

    public static class A1Mapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[]split=line.split(",");
            //Perform the string replacement
//            String replaced = line.replace("19-Dec", "12-19");
//            //Write the replaced string to the output
//            context.write(new Text(line), new Text(replaced));


            for (int i = 0; i < split.length ; i++) {
                if(split[i].equals("19-Dec")) {
                    split[i] = "12-19";
                context.write();
                }
            }

//            String two_string = itr.nextToken();
//            int one_integer = Integer.parseInt(one_string);
//            int two_integer = Integer.parseInt(two_string);

            /* << your code here >> */
//            context.write(new IntWritable(one_integer), new IntWritable(two_integer));
//            context.write(new IntWritable(two_integer), new IntWritable(one_integer));
        }
    }

    public static class A1Reducer
            extends Reducer<Text,Text,Text,Text> {


        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val:values ) {
                context.write(key,val);
            }



        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: A1 <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Yu Fo Hon 20201702");
        job.setJarByClass(FriendsOfFriends.class);
        job.setMapperClass(A1Mapper.class);
        job.setReducerClass(A1Reducer.class);
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
