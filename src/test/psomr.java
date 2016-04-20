package test;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class psomr 
{
    public static void main(String[] args) throws Exception 
    {
          Configuration conf = new Configuration();
          String[]otherArgs = new GenericOptionsParser(conf, args)
                 .getRemainingArgs();
           //if(otherArgs.length != 2) 
           //{
           //   System.err.println("Usage:wordcount <in><out>");
           //   System.exit(2);
           //}
           String input = "hdfs://192.168.1.100:9000/file.txt";
           String output = "hdfs://192.168.1.100:9000/0";
           FileSystem fs;

           try
           {
              fs =FileSystem.get(conf);
              int step = 5;
              for(int i = 0; i< step; i++) 
              {
                 System.out.println("第" + i + "次：" + i);
                 Job job = new Job (conf, "word count");
                 job.setJarByClass(psomr.class);
                 
                 job.setMapperClass(IntSumReducer.class);
                 job.setCombinerClass(job1Reducer.class);
                 job.setReducerClass(job1Reducer.class);
                 
                 job.setMapOutputKeyClass(DoubleWritable.class);
                 job.setMapOutputValueClass(Text.class);
                 
                 job.setOutputKeyClass(Text.class);
                 job.setOutputValueClass(Text.class);
                 FileInputFormat.addInputPath(job, new Path(input));
                 FileOutputFormat.setOutputPath(job, new Path(output));
                 
                 job.waitForCompletion(true);
                 
                 String in = String.valueOf(i);
                 String out = String.valueOf(i+1);
                 input = "hdfs://192.168.1.100:9000/" + in +"/part-r-00000"; 
                 output = "hdfs://192.168.1.100:9000/" + out;
              }
           } 
           catch(IOException e) 
           {
              e.printStackTrace();
           } 
           catch(InterruptedException e) 
           {
              e.printStackTrace();
           } 
           catch(ClassNotFoundException e) 
           {
              e.printStackTrace();
           }
       }
}
