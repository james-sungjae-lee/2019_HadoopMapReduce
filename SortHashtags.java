package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SortHashtags {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /*
         * Job 1: Count the number of genres appeared
         */
        Job job1 = Job.getInstance(conf, "HashtagCount");
        job1.setJarByClass(SortHashtags.class);

        job1.setMapperClass(HashtagCounterMap.class);
        job1.setReducerClass(HashtagCounterReduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        job1.waitForCompletion(true);

        /*
         * Job 2: Sort based on the number of occurences
         */
        Job job2 = Job.getInstance(conf, "SortByCountValue");

        job2.getConfiguration().set("knum", args[2]);

        job2.setNumReduceTasks(1);

        job2.setJarByClass(SortHashtags.class);

        job2.setMapperClass(SortByValueMap.class);
        job2.setReducerClass(SortByValueReduce.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        job2.waitForCompletion(true);
    }

    public static class HashtagCounterMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text hashtag = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            for (String word : words) {
                if(word.length() > 1 && word.startsWith("#")){
                    hashtag.set(word);
                    context.write(hashtag, one);
                }
            }
        }
    }

    public static class HashtagCounterReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class SortByValueMap extends Mapper<Text, Text, IntWritable, Text> {
        private Text word = new Text();
        IntWritable frequency = new IntWritable();

        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            frequency.set(Integer.parseInt(value.toString()));
            context.write(frequency, key);
        }
    }

    public static class SortByValueReduce extends Reducer<IntWritable, Text, Text, IntWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String knum = conf.get("knum");
            int int_knum = Integer.parseInt(knum);
            int counter = 0;

            for (Text value : values) {
                if(key > int_knum){
                    context.write(value, key);
                }
            }
        }
    }
}
