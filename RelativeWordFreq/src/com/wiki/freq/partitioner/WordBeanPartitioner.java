package com.wiki.freq.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.wiki.freq.bean.WordBean;

public class WordBeanPartitioner extends Partitioner<WordBean,IntWritable> {

    @Override
    public int getPartition(WordBean wordPair, IntWritable intWritable, int numPartitions) {
        return (wordPair.getWord().hashCode() & Integer.MAX_VALUE ) % numPartitions;
    }
}
