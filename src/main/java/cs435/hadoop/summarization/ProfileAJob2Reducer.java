package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.io.IOException;
import java.util.TreeMap;

public class ProfileAJob2Reducer extends Reducer<IntWritable, Text, IntWritable, Text>{
    
    //key: documentID, value: unigram \t frequency
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   
        Map<String, Integer> unigramFrequency = new HashMap<>();
        double maxFrequency = 0;
        
        //Extract all unigrams and frequencies and find the max frequency
        for(Text val : values){
            String[] line = val.toString().split("\t");
            String unigram = line[0];
            int frequency = Integer.parseInt(line[1]);
            
            if(unigramFrequency.containsKey(unigram)){
                int count = unigramFrequency.get(unigram);
                count++;
                unigramFrequency.put(unigram, count);
            } else {
                unigramFrequency.put(unigram, 1);
            }
            
            if(frequency > maxFrequency){
                maxFrequency = frequency;
            }
        }
        
        //Calculate TF for each unigram
        for(String unigram : unigramFrequency.keySet()){
            double TF = 0.5 + (0.5 * (unigramFrequency.get(unigram) / maxFrequency));
            String output = unigram + "\t" + TF;
            context.write(key, new Text(output));
        }
    }
}
