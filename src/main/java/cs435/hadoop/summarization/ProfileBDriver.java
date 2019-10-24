package cs435.hadoop;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class ProfileBDriver {
    
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        
        
        //JOB 1
        Job job1 = Job.getInstance(conf1, "Profile B Job 1");
        
        job1.setJarByClass(ProfileBDriver.class);
        job1.setMapperClass(ProfileBDocumentsMapper.class);
        job1.setMapperClass(ProfileBCacheMapper.class);
        job1.setReducerClass(ProfileBReducer.class);

        //Output from identity reducer 
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        
        //Set number of reducers to 10
        job1.setNumReduceTasks(10);
        
        //Path to input and output HDFS
//         FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        
        //Multiple Inputs 
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, ProfileBDocumentsMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, ProfileBCacheMapper.class);
        
        if (!job1.waitForCompletion(true)) {
        System.exit(1);
        }        
    }
}
