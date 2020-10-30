package com.wiki.freq.reducer;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.wiki.freq.bean.WordBean;

public class WordReducer2 extends Reducer<DoubleWritable,WordBean,WordBean,DoubleWritable> {
    private int i = 0;
    @Override
    protected void reduce(DoubleWritable key, Iterable<WordBean> values, Context context) throws IOException, InterruptedException {
       
        for (WordBean value : values) {

          if(i >= 100)
              break;

          if(key.get() == 1.0)
             continue;

          context.write(value,key);
          i++;
        }
    }
}
