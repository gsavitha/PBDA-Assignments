import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class q3 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: q3 <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "q3");
        job.setJarByClass(q3.class);
        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(ZInputFormat.class);

        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MyMapper extends Mapper<Text, BytesWritable, Text, Text>{
        public void map(Text key, BytesWritable value, Context context
        ) throws IOException, InterruptedException {

                String content = new String(value.getBytes(),"UTF-8");
                int i=0;
                for(String s:content.split("\n")) {
                    i+=1;


                    if(s.toLowerCase().contains("door")) {
                        System.out.println(key);
                        context.write(new Text(key.toString()+"\t" + String.valueOf(i) +"\t,"),new Text(s));
                            //context.write(key,new Text(s));
                    }
                }
            }
        }

    public static class MyReducer extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key,val);
            }

        }
    }

}