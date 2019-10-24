package cs435.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;

public class ProfileAJob2Mapper extends Mapper<Object, Text, IntWritable, Text> {
    
    //Value: documentID \t unigram \t frequency
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t", 3);
        int documentID = Integer.parseInt(line[0]);
        String output = line[1] + "\t" + line[2];
        
        context.write(new IntWritable(documentID), new Text(output));
    }
}
