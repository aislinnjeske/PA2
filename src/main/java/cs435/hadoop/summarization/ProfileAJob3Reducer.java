package cs435.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.lang.Math;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;

public class ProfileAJob3Reducer extends Reducer<Text, Text, IntWritable, Text>{
    
    private long N;
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        JobClient client = new JobClient((JobConf) conf);
        RunningJob parentJob = client.getJob((org.apache.hadoop.mapred.JobID) JobID.forName( conf.get("mapred.job.id") ));
        N = parentJob.getCounters().getCounter(ProfileADriver.DocumentsCount.NUMDOCS);
    }

    //Key: unigram, Value: documentID \t TF value
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Double> documents = new TreeMap<>();

        for(Text val : values){
            String[] line = val.toString().split("\t");
            int documentID = Integer.parseInt(line[0]);
            double TF = Double.parseDouble(line[1]);
            documents.put(documentID, TF);
        }

        int n = documents.size();

        for(Integer documentID : documents.keySet()){
            double IDF = Math.log10(N/n);
            double TF = documents.get(documentID);
            double TF_IDF = IDF * TF;
            context.write(new IntWritable(documentID), new Text(key.toString() + "\t" + TF_IDF));
        }
    }
}
