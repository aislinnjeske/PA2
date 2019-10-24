package cs435.hadoop;

import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;


public class ProfileAJob3Mapper extends Mapper<Object, Text, Text, Text> {
    
    public static Set<Integer> documents = new HashSet<>();
    
    //Value: documentID \t unigram \t TF value
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t", 3);
        int documentID = Integer.parseInt(line[0]);
        String unigram = line[1];
        double TF = Double.parseDouble(line[2]);
        
        //If we haven't already seen this document, count it
        if(documents.add(documentID)){
            context.getCounter(ProfileADriver.DocumentsCount.NUMDOCS).increment(1);
        }
        
        context.write(new Text(unigram), new Text(documentID + "\t" + TF));
    }
}
