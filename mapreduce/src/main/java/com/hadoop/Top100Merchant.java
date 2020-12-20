package com.hadoop;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class Top100Merchant {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {


        private final static IntWritable one = new IntWritable(1);

        private Set<String> userYouth = new HashSet<String>();
        protected void setup(Context context) throws IOException,
                InterruptedException {
            URI[] userURIs = context.getCacheFiles();
            for (URI userURI : userURIs) {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(userURI.getPath()),"UTF-8"));
                String line;
                while((line=br.readLine())!=null){

                    if (!line.contains(",,")){
                        String[] split = line.split(",");
                        if(split[1].equals("1") || split[1].equals("2") || split[1].equals("3")){
                            userYouth.add(split[0]);
                        }
                    }
                }
                IOUtils.closeStream(br);
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if(!tokens[6].equals("0") && tokens[5].equals("1111") && userYouth.contains(tokens[0])){
                StringBuffer sb = new StringBuffer();
                sb.append(tokens[0]).append(",");
                sb.append(tokens[1]).append(",");
                sb.append(tokens[3]).append(",");
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
            String item = line.split(",")[2];
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
        Job job1 = Job.getInstance(conf, "deduplication");
        job1.setJarByClass(Top100Merchant.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.addCacheFile(new Path(args[2]).toUri());

        Path tempDir1 = new Path("temp2/temp-output1");
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempDir1);
        job1.waitForCompletion(true);


        Job job2 = Job.getInstance(conf, "wordcount");
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, tempDir1);
        Path tempDir2 = new Path("temp2/temp-output2");
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, tempDir2);
        job2.waitForCompletion(true);

        Job sort = Job.getInstance(conf,"sort");//定义第二个Job
        sort.setJarByClass(Top100Merchant.class); //将第二个Job的输入路径设为临时路径
        FileInputFormat.addInputPath(sort, tempDir2);
        sort.setInputFormatClass(SequenceFileInputFormat.class);
        sort.setMapperClass(InverseMapper.class); //交换key-value对的key和value，利用reduce的排序
        sort.setReducerClass(Top100Merchant.sortReducer.class);//对第二个Job使用新的Reducer
        sort.setNumReduceTasks(1);//设置reduce个数为1
        FileOutputFormat.setOutputPath(sort,new Path(args[1]));//将第二个Job的输出路径设为参数中的输出路径
        sort.setOutputKeyClass(IntWritable.class);
        sort.setOutputValueClass(Text.class);
        sort.setSortComparatorClass(Top100Merchant.mycamp.class);//通过自定义的比较函数，将reduce自带的排序功能升序变为降序
        System.exit(sort.waitForCompletion(true) ? 0 : 1);//程序结束

    }

}
