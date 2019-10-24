package cs435.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class ProfileADriver {

    public static enum DocumentsCount {
        NUMDOCS
    };
    
    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = new Configuration();
            
            //JOB 1
            Job job1 = Job.getInstance(conf, "Profile A Job 1");
            
            job1.setJarByClass(ProfileADriver.class);
            job1.setMapperClass(ProfileAJob1Mapper.class);
            job1.setReducerClass(ProfileAJob1Reducer.class);
            
            //Output from reducer 
            job1.setOutputKeyClass(IntWritable.class);
            job1.setOutputValueClass(Text.class);
            
            //Set number of reducers to 10
            job1.setNumReduceTasks(10);
            
            //Path to input and output HDFS
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            
            job1.waitForCompletion(true);
            
            
            //JOB 2
            Job job2 = Job.getInstance(conf, "Profile A Job 2");

            job2.setJarByClass(ProfileADriver.class);
            job2.setMapperClass(ProfileAJob2Mapper.class);
            job2.setReducerClass(ProfileAJob2Reducer.class);
            
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);
            
            job2.setNumReduceTasks(10);
            
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            
            job2.waitForCompletion(true);
            
            
            //JOB 3
            Job job3 = Job.getInstance(conf, "Profile A Job 3");
            
            job3.setJarByClass(ProfileADriver.class);
            job3.setMapperClass(ProfileAJob3Mapper.class);
            job3.setReducerClass(ProfileAJob3Reducer.class);

            //Output from mapper
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);

            //Output from reducer 
            job3.setOutputKeyClass(IntWritable.class);
            job3.setOutputValueClass(Text.class);
            
            //Set number of reducers to 10
            job3.setNumReduceTasks(10);
            
            //Path to input and output HDFS
            FileInputFormat.addInputPath(job3, new Path(args[2]));
            FileOutputFormat.setOutputPath(job3, new Path(args[3]));
                    
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
            
        } catch(IOException | InterruptedException | ClassNotFoundException e){
            System.err.println(e.getMessage());
        }
    }
}
