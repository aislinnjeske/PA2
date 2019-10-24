package cs435.hadoop;

import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;

public class ProfileBCacheMapper extends Mapper<Object, Text, IntWritable, Text> {
    
    //Value: documentID \t unigram \t tf-idf value
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {        
        
        //Split by \t
        String[] line = value.toString().split("\t", 3);
        int documentID = Integer.parseInt(line[0]);
        
        context.write(new IntWritable(documentID), new Text("C" + line[1] + ":" + line[2]));
    }
}
