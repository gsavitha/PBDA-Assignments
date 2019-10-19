import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class q4 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: q4 <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "q4");
        job.setJarByClass(q4.class);
        job.setMapperClass(Mapperq4.class);
        job.setReducerClass(Reducerq4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(JSONInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Mapperq4 extends Mapper<Text, Text, Text, Text>{
        public void map(Text key,Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }


    public static class Reducerq4 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key,(new Text(val.toString().replaceAll("\t","").replaceAll("\n",""))));
            }

        }
    }

}