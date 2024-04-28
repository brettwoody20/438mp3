package problem.two;

import java.io.BufferedReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class MyRecordReader extends RecordReader<IntWritable, Text> {
    
    private IntWritable key = null;
    private Text value = null;
    private FileSplit fsplit;
    private Configuration conf;
    private FSDataInputStream fsinstream;
    private FileSystem fs;
    private long splitstart = 0;
    private long splitend = 0;
    private long bytesread = 0;
    private BufferedReader br;
    private final int newlinebytes = ("\n").getBytes().length;
    private boolean endofsplit = true;

    // Record boundary in this case is when a line which is not numeric is follwed by 
    // numeric line. So to skip a record, we read till we hit numeric line or EOF. Since
    // we exit only after hitting the numeric line, we need to set the key as well in same 
    // code.
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException {
        this.fsplit = (FileSplit) split;
        this.conf = context.getConfiguration();

        fs = fsplit.getPath().getFileSystem(conf);

        fsinstream = fs.open(fsplit.getPath());
        fsinstream.skip(fsplit.getStart());

        splitstart = fsplit.getStart();
        splitend = splitstart + fsplit.getLength();

        br = new BufferedReader(new InputStreamReader(fsinstream));
        if (splitstart != 0) {
            skipFirstRecordAndSetKey();
        }
        else 
            endofsplit = false;
    }

    public void skipFirstRecordAndSetKey() throws IOException {
        String skipline;
        // We have to skip the first line any which way, as it may be partial line from 
        // previous split.
        if ((skipline = br.readLine()) != null) {
            bytesread += (skipline.getBytes().length + newlinebytes);
        
        //  We skip till we hit next key
            while (((skipline = br.readLine()) != null)
                    && !(StringUtils.isNumeric(skipline))) {
                bytesread += (skipline.getBytes().length + newlinebytes);
            }
        }
        if (((bytesread + splitstart) <= splitend) && skipline != null) {
            key = new IntWritable(Integer.parseInt(skipline));
            bytesread += (skipline.getBytes().length + newlinebytes);
            endofsplit = false;
        }
    }
        
    //  We call this function recursively if we hit consecutive numeric lines otherwise
    //  we just return key value. Pay attention, we only return true when we hit a value,
    //  but not when we hit key. Because this new key has to be paired with the next value
    //  which will be read.    
        @Override
        public boolean nextKeyValue() throws IOException {
            String line;
            if ((line = br.readLine()) != null && !(endofsplit)) {

                if (StringUtils.isNumeric(line)) {
                    key = new IntWritable(Integer.parseInt(line));
                    if ((splitstart + bytesread) > splitend) {
                        return false;
                    } else {
                        bytesread += (line.getBytes().length + newlinebytes);
                        return nextKeyValue();
                    }
                } else {
                    value = new Text(line);
                    bytesread += (line.getBytes().length + newlinebytes);
                    return true;
                }
            } else {
                return false;
            }
        }

    @Override
            public void close() throws IOException {
    // do nothing
        }

        @Override
            public IntWritable getCurrentKey() {
            return this.key;
        }

        @Override
            public Text getCurrentValue() {
            return this.value;
        }

        @Override
            public float getProgress() throws IOException {
            return true ? 1.0f : 0.0f;
        }
}
