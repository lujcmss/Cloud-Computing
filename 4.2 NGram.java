package edu.cmu;

import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.*;

public class NGram {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
      private Text word = new Text();
      private final static IntWritable one = new IntWritable(1);

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokenizer = value.toString().split("[^a-zA-Z]+");
	int start = 0;
	if (tokenizer.length > 0 && tokenizer[0].equals("")) {
	  start++;
	}       
        for (int i = start; i < tokenizer.length; i++) {
          StringBuilder sb = new StringBuilder();
          for (int j = 0; j < 5; j++) {
            if (i+j < tokenizer.length) {
              if (j > 0) {
                sb.append(" ");
              }
              sb.append(tokenizer[i+j].toLowerCase());
              word.set(sb.toString());
              context.write(word, one);
            }
          }
        }
      }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }

        context.write(key, new IntWritable(sum));
      }
    }

    public static void main(String[] args) throws Exception {
      Job job = new Job();
      job.setJarByClass(edu.cmu.NGram.class);
      job.setJobName("NGram");

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.waitForCompletion(true);
    }
}

