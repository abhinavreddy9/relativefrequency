package com.wiki.freq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.wiki.freq.bean.WordBean;
import com.wiki.freq.mapper.WordMapper1;
import com.wiki.freq.mapper.WordMapper2;
import com.wiki.freq.partitioner.WordBeanPartitioner;
import com.wiki.freq.reducer.WordBeanCombineReducer;
import com.wiki.freq.reducer.WordReducer1;
import com.wiki.freq.reducer.WordReducer2;
import com.wiki.freq.utils.DescendingKeyComparator;

import java.io.IOException;

public class RelativeWikiTextFreqStart {

    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {

        setupJob1(args[0]);
        setupJob2(args[1]);
    }


	private static void setupJob1(String inputFilePath) throws IOException, InterruptedException, ClassNotFoundException {
		Job job1 = Job.getInstance(new Configuration());
        job1.setJarByClass(RelativeWikiTextFreqStart.class);
        job1.setJobName("RelativeWikiTextFrequencies");

        FileInputFormat.addInputPath(job1, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job1, new Path("intermediateOutput"));

        job1.setMapperClass(WordMapper1.class);
        job1.setReducerClass(WordReducer1.class);
        job1.setCombinerClass(WordBeanCombineReducer.class);
        job1.setPartitionerClass(WordBeanPartitioner.class);
        job1.setNumReduceTasks(3);

        job1.setOutputKeyClass(WordBean.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.waitForCompletion(true);
	}
	
	private static void setupJob2(String outputFilePath) throws IOException, InterruptedException, ClassNotFoundException {
		Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(RelativeWikiTextFreqStart.class);
        job2.setJobName("RelativeWikiTextFrequencies");

        job2.setSortComparatorClass(DescendingKeyComparator.class);
        FileInputFormat.addInputPath(job2, new Path("intermediateOutput"));
        FileOutputFormat.setOutputPath(job2, new Path(outputFilePath));

        job2.setMapperClass(WordMapper2.class);
        job2.setReducerClass(WordReducer2.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(WordBean.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
