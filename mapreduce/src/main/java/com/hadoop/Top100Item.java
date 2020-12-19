package com.hadoop;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class Top100Item {
    public static class Mapper1
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if(!tokens[6].equals("0") && tokens[5].equals("1111")){
                StringBuffer sb = new StringBuffer();
                sb.append(tokens[0]).append(",");
                sb.append(tokens[1]).append(",");
                sb.append(tokens[6]).append(",");

                context.write(new Text(sb.toString()), one);
            }

        }
    }

    public static class Reducer1
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Mapper2
            extends Mapper<LongWritable, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().split("\t")[0];
            String item = line.split(",")[1];
            context.write(new Text(item), one);
        }

    }

    public static class Reducer2
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    private static class mycamp extends IntWritable.Comparator {//自定义比较函数以实现降序排列
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    public static class sortReducer//排序Job使用的Reducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private int num=1;  //计数
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text value : values) {
                //只给出前100个符合格式的键值对
                if (num<=100) {
                    context.write(new IntWritable(num),new Text(value.toString()+", "+key.toString()));
                    num++;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "deduplicatioin");
        job1.setJarByClass(Top100Item.class);
        job1.setMapperClass(Mapper1.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path tempDir1 = new Path("temp1/temp-output1");
        FileOutputFormat.setOutputPath(job1, tempDir1);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "wordcount");
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job2, tempDir1);
        Path tempDir2 = new Path("temp1/temp-output2");
        FileOutputFormat.setOutputPath(job2, tempDir2);
        job2.waitForCompletion(true);


        Job sort = Job.getInstance(conf,"sort");//定义第二个Job
        sort.setJarByClass(Top100Item.class); //将第二个Job的输入路径设为临时路径
        FileInputFormat.addInputPath(sort, tempDir2);
        sort.setInputFormatClass(SequenceFileInputFormat.class);
        sort.setMapperClass(InverseMapper.class); //交换key-value对的key和value，利用reduce的排序
        sort.setReducerClass(sortReducer.class);//对第二个Job使用新的Reducer
        sort.setNumReduceTasks(1);//设置reduce个数为1
        FileOutputFormat.setOutputPath(sort,new Path(args[1]));//将第二个Job的输出路径设为参数中的输出路径
        sort.setOutputKeyClass(IntWritable.class);
        sort.setOutputValueClass(Text.class);
        sort.setSortComparatorClass(mycamp.class);//通过自定义的比较函数，将reduce自带的排序功能升序变为降序
        System.exit(sort.waitForCompletion(true) ? 0 : 1);//程序结束

    }
    /*
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if(!tokens[6].equals("0") && tokens[5].equals("1111")){
                word.set(tokens[1]);
                context.write(word, one);
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Top100Item.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path tempDir = new Path("temp-output");
        FileOutputFormat.setOutputPath(job, tempDir);

        job.waitForCompletion(true);

        Job sortjob = Job.getInstance(conf, "sort");
        FileInputFormat.addInputPath(sortjob, tempDir);
        sortjob.setInputFormatClass(SequenceFileInputFormat.class);
        sortjob.setMapperClass(InverseMapper.class);
        sortjob.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(sortjob,
                new Path(args[1]));
        sortjob.setOutputKeyClass(IntWritable.class);
        sortjob.setOutputValueClass(Text.class);
        sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);

        sortjob.waitForCompletion(true);
        System.exit(0);
    }

     */
}
