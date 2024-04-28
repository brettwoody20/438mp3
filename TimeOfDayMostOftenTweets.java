import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TimeOfDayMostOftenTweets {
    public static class TweetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\n");
            if (lines.length >= 4) {
                try {
                    // Parse timestamp
                    String timestampString = lines[0].split(" ")[1];
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(dateFormat.parse(timestampString));
                    int hour = cal.get(Calendar.HOUR_OF_DAY);

                    // Emit hour as key and value 1
                    context.write(new Text(String.valueOf(hour)), new IntWritable(1));
                } catch (ParseException e) {
                    // Ignore invalid timestamps
                }
            }
        }
    }

    public static class TweetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Time of Day Most Often Tweets");
        job.setJarByClass(TimeOfDayMostOftenTweets.class);
        job.setMapperClass(TweetMapper.class);
        job.setReducerClass(TweetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

