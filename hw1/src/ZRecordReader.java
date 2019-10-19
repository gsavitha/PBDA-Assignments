import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */
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
public class ZRecordReader extends RecordReader<Text, BytesWritable> {

    private FSDataInputStream fsin;
    private ZipInputStream zip;
    private Text currentKey;
    private BytesWritable currentValue;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        Path path = ((FileSplit) inputSplit).getPath();
        FileSystem fs = path.getFileSystem( conf );
        fsin = fs.open(path);
        zip = new ZipInputStream(fsin);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        ZipEntry entry = null;
        try
        {
            entry = zip.getNextEntry();
        }
        catch ( ZipException e )
        {

        }


        if ( entry == null )
        {
            return false;
        }

        // Filename
        currentKey = new Text( entry.getName() );

        // Read the file contents
        ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
        byte[] val = new byte[2048];
        while (true)
        {
            int bytesRead = 0;
            try{
            bytesRead = zip.read( val, 0, 2048 );
            }
            catch(Exception e1){

            }
            if ( bytesRead > 0 )
                bytestream.write( val, 0, bytesRead );
            else
                break;
        }
        zip.closeEntry();

        // Uncompressed contents
        currentValue = new BytesWritable( bytestream.toByteArray() );
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        zip.close();
        fsin.close();
    }
}