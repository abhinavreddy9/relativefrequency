package com.wiki.freq.reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.wiki.freq.bean.WordBean;

import java.io.IOException;

public class WordBeanCombineReducer extends Reducer<WordBean,IntWritable,WordBean,IntWritable> {
    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(WordBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        totalCount.set(count);
        context.write(key,totalCount);
    }
}
