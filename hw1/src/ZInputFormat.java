import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.zip.ZipInputStream;

/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */
public class ZInputFormat extends FileInputFormat<Text, BytesWritable> {

   // @Override
//    public List<InputSplit> getSplits(JobContext context)
//            throws IOException {
//
//
//        // your code here
//        // our splits will consist of whole files found insize our input zip file
//        // return splits....
//        return null;
//    }


        @Override
        protected boolean isSplitable( JobContext context, Path filename )
        {
            return false;
        }


    /*** return a record reader
     *
     * @param split
     * @param context
     * @return (Text,BytesWritable)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        return new ZRecordReader();
    }
}