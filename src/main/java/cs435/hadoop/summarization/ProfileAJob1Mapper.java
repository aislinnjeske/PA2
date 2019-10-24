package cs435.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;

public class ProfileAJob1Mapper extends Mapper<Object, Text, IntWritable, Text> {
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(isInvalidLine(value.toString())){
            return;
        }
        
        //Line format: Title <====> Unique Document ID <====> Contents
        String[] line = value.toString().split("<====>", 3);
        int documentID = Integer.parseInt(line[1]);
        
        StringTokenizer itr = new StringTokenizer(line[2]);
        
        if(isInvalidLine(line[2])){
            return;
        }
        
        while(itr.hasMoreTokens()){
            String word = itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z0-9]+","");
            
            if(word.length() > 0){
                context.write(new IntWritable(documentID), new Text(word));
            }
        }
    }
    
    private boolean isInvalidLine(String line){
        return line == null || line.length() == 0;
    }
}
