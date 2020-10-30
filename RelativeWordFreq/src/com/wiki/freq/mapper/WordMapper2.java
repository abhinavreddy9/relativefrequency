package com.wiki.freq.mapper;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.wiki.freq.bean.WordBean;

 
public class WordMapper2
       extends Mapper<Object, Text, DoubleWritable, WordBean>{

        private String[] str;
        private String[] tokens;
        private DoubleWritable relativeFreq = new DoubleWritable();
 
        public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

           StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
           while (itr.hasMoreTokens()) {
      	      tokens = itr.nextToken().toString().split("\t");
      	      str = tokens[0].toString().split(" ");
              WordBean wordpair = new WordBean(str[0], str[1]);
      	      relativeFreq.set(Double.parseDouble(tokens[1].trim()));
              if(relativeFreq == null)
                  continue;
              context.write(relativeFreq, wordpair);
          }
        }
}
