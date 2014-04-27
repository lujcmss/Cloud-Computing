package edu.cmu;

import java.io.*;
import java.util.*;
import java.util.PriorityQueue;
import java.net.URI;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.util.*;

public class Wordpair {
    public static class Map extends Mapper<Object, Text, ImmutableBytesWritable, Text> {
      private Text next = new Text();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String[] tokenizer = value.toString().split("\t");
	int t = Integer.parseInt(context.getConfiguration().get("t"));

	int index = tokenizer[0].lastIndexOf(" ");
	int freq = Integer.valueOf(tokenizer[1]);

	if (index != -1 && freq >= t) {    
	  next.set(tokenizer[0].substring(index+1)+"*"+tokenizer[1]);
          context.write(new ImmutableBytesWritable(tokenizer[0].substring(0, index).getBytes()), next);
        }
      }
    }

    public static class Reduce extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

      public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Comparator<String> cmp = new WordpairComparator();
        int n = Integer.parseInt(context.getConfiguration().get("n"));
	PriorityQueue<String> queue = new PriorityQueue<String>(n+1, cmp);

	for (Text val : values) {
          String v = val.toString();
	  queue.add(v);
	  if (queue.size() > n) {
	    queue.poll();
	  }
        }

	while (queue.size() != 0) {
	  String tmp = queue.poll();
	  String[] sp = tmp.split("[*]");
	  Put put = new Put(key.get());
	  put.add("wordpairs".getBytes(), sp[0].getBytes(), sp[1].getBytes());
          context.write(key, put);
	}
      }
    }
    
    private static class WordpairComparator implements Comparator<String> {
      @Override
      public int compare(String x, String y) {
	int freqx = Integer.valueOf(x.split("[*]")[1]);
	int freqy = Integer.valueOf(y.split("[*]")[1]);
	
	if (freqx < freqy) {
	  return -1;
	} else if (freqx > freqy) {
	  return 1;
	} else {
	  return 0;
	}
      }
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = HBaseConfiguration.create();
      
      int x = 0;
      boolean flag = true;
      while (flag) {
	if (args[x].equals("-t")) {
	  conf.set("t", args[x+1]);
	  x += 2;
	} else if (args[x].equals("-n")) {
	  conf.set("n", args[x+1]);
	  x += 2;
	} else {
	  flag = false;
	}
      }     

      Job job = new Job(conf, "lmg");
      job.setJarByClass(edu.cmu.Wordpair.class);
      job.setJobName("Wordpair");

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(Text.class);

      job.setOutputKeyClass(ImmutableBytesWritable.class);
      job.setOutputValueClass(Text.class);

      TableMapReduceUtil.initTableReducerJob("wp", Reduce.class, job);
      FileInputFormat.addInputPath(job, new Path(args[x]));
      FileOutputFormat.setOutputPath(job, new Path(args[x+1]));
      
      job.waitForCompletion(true);
    }
}

