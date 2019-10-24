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

public class ProfileBDocumentsMapper extends Mapper<Object, Text, IntWritable, Text> {
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {        
        String[] line = value.toString().split("<====>");

        if(line.length != 3){
        return;
        }        

        if(line[2].length() == 0){
        return;
        } 
        int documentID = Integer.parseInt(line[1]);
        context.write(new IntWritable(documentID), new Text("D" + line[2]));
    }
}
