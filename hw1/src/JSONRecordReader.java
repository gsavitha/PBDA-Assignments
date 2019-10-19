import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Stack;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;



public class JSONRecordReader extends RecordReader<Text, Text> {

    private FSDataInputStream fsin;
    private Text currentKey;

    private Text currentValue;

    private long start;
    private long end;

    private int record;
    private String filename;

    Log log = LogFactory.getLog(q4.Mapperq4.class);
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        FileSplit fsplit= ((FileSplit) inputSplit);
        Path path = fsplit.getPath();
        FileSystem fs = path.getFileSystem( conf );
        fsin = fs.open(path);
        String p=path.toString();
        filename = p.substring(p.lastIndexOf('/') + 1);
        record=0;
        start = fsplit.getStart();
        end = fsplit.getLength() - 1;
        log.info(end);
        log.debug("#\n#\n#\n#\n#########3");
//
//        input = fs.open(fileSplit.getPath());
//        input.seek(startByte);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        long count=0;

        long jsonstart=fsin.getPos();

        //System.out.println(end);
        if (jsonstart <= end) {

            byte[] c = new byte[1];

            fsin.readFully(c);
            String temp = new String(c,"UTF-8");
            while (!temp.equals("{") && fsin.getPos() <= end ){
                fsin.readFully(c);
                temp=new String(c,"UTF-8");
                System.out.println(temp);


            }
            if (temp.equals("{")) {

                    if(count==0)
                        jsonstart = fsin.getPos();
                count += 1;
                while (fsin.getPos() <= end) {
                    fsin.readFully(c);
                    temp=new String(c,"UTF-8");
                    if (temp.equals("}")) {
                        if (count == 1) {

                            long jsonend = fsin.getPos();
                            int len = (int) (jsonend - jsonstart);

                            byte[] buffer = new byte[len];
                            fsin.readFully( (int) jsonstart-1,buffer,0, len);
                            currentValue = new Text(buffer);
                            currentKey = new Text(filename +"\t" + new Text(String.valueOf(record)));
                            record++;

                            return true;
                        } else {

                            count--;
                        }
                    }
                }

            }
//            else if (count != 0) {
//                throw new RuntimeException("Couldn't parse JSON. Unexpected Format");
//            }
        }
    return false;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        fsin.close();
    }
}