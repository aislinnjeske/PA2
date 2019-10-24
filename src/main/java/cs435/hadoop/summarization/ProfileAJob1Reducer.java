package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.io.IOException;
import java.util.TreeMap;

public class ProfileAJob1Reducer extends Reducer<IntWritable, Text, IntWritable, Text>{
    
    //key = document ID, value = word
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   
        Map<String, Integer> unigramFrequency = new HashMap<>();
        
        for(Text val : values){
            String unigram = val.toString();
            
            if(unigramFrequency.containsKey(unigram)){
                int count = unigramFrequency.get(unigram);
                count++;
                unigramFrequency.put(unigram, count);
            } else {
                unigramFrequency.put(unigram, 1);
            }
        }
        
        //output: key = document ID, value = unigram \t frequency
        for(String unigram : unigramFrequency.keySet()){
            String value = unigram + "\t" + unigramFrequency.get(unigram);
            context.write(key, new Text(value));
        }
    }
}
