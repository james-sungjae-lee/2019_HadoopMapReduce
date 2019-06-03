package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;

// Hadoop Configuration 과 file system 의 Path 기능을 가져온다.
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// Long Writable 과 IntWritable 의 정확한 차이는?
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// Job(driver), Mapper, Reducer 의 3가지 기능을 가져온다
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// File 을 Text 형태로 읽어오고, File 을 Text 형태로 작성하는 의미로 보인다
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// Happy 단어가 들어있는지 검사하는 HappContain MR 코드를 작성한다
public class HappyContain{
    public static void main(String[] args) throws Exception{

        // Configuration 과 Job 을 생성한다.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "happycontain");

        job.setJarByClass(HappyContain.class);

        // Lab2 에서는 특별하게 Map 의 Output Key,Value Class 를
        // Text, LongWritable 로 따로 지정해주는 것이 필요하다.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Reduce 는 기본 Output Key,Value Class 인 Text 로 지정한다.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

	// set Mapper and Reducer class to Map and Reduce
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

        // InputFormat 과 OutputFormat 을 Text Format 으로 지정한다.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 결과물을 File 형태로 저장할 수 있도록 File Input, Output Format 을 지정한다.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // job 이 완료되는 것을 기다린다.
        job.waitForCompletion(true);
    }

    // Map 작업은 LongWritable 과 Text 를 입력으로 받아,
    // Text 와 LongWritable 형태로 key - value pair 를 Reduce 에 넘겨주게 된다.
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>{
        // 반환하고자 하는 Key 값은 happy 단어가 들어있는 tweet 이므로, Text 객체로 생성한다.
        private Text happy_tweet = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            // 전체 텍스트의 각 line 을 String 변환 하여 tweet 으로 저장한다.
            // 만약 해당 String 에 "happy" 가 존재하면, happy_tweet 으로 만들어 context 에 저장한다.
            String tweet = value.toString();
            if(tweet.toLowerCase().contains("happy")){
                happy_tweet.set(tweet);
                context.write(happy_tweet, key);
            }
        }
    }

    // Reduce 작업은 앞에서 반환된 Text, LongWritable 을 입력으로 받아
    // Text, Text 형태로 key-value pair 를 File 에 저장하는 작업을 진행한다.
    public static class Reduce extends Reducer<Text, LongWritable, Text, Text>{
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{

            // 여러 String 객체를 하나로 만드는 작업을 위해서, StringBuilder 를 사용한다.
            // 먼저 입력받은 values 는 LongWritable 형태로 Interable 한 객체이다.
            // 여기에는 동일한 tweet 이 존재하는 여러 개의 ID 가 저장된다.
            StringBuilder idListBuilder = new StringBuilder();

            // 최종적으로 value 로 출력되는 Text 는 id 의 List 를 가지게 된다.
            Text idList = new Text();

            // idListBuilder 에는 [ a, b, c ] 형태로 ID 가 저장되게 된다.
            // 만약 idListBuilder 에 어떠한 ID가 앞에 존재한다면, ", " 를 이용해 다음 ID 와 구분한다.
            // value 는
            idListBuilder.append("[ ");
            for(LongWritable value : values){
                if(idListBuilder.length() > 2){
                    idListBuilder.append(", ");
                }
                idListBuilder.append(String.valueOf(value.get()));
            }
            idListBuilder.append(" ]");

            // 현재 idListBuilder 는 StringBuilder 객체이기 때문에, String 으로 변환 후 Text 로 만들어야 한다.
            // 만들어진 Text 객체는 context 에 write 하는 것으로 끝난다.
            idList.set(idListBuilder.toString());
            context.write(key, idList);
        }
    }
}
