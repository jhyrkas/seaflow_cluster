package emmapreduce;

import java.io.IOException;

import mrresults.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AstroDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	    Job job = new Job(super.getConf());
	    job.waitForCompletion(true);
	    return 0;
	}
	
	public static void runIter(Path inputpath, Path outputpath, Path cachepath, int k, int iter, String[] args) throws IOException {
	    System.out.println("in runIter");
		JobConf conf = new JobConf(AstroDriver.class);
		conf.setJobName("EM Map Reduce Iteration: " + iter);
			
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
			
		conf.setMapperClass(EMMapper.class);
		conf.setCombinerClass(EMCombiner.class);
		conf.setReducerClass(EMReducer.class);
	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(EMOutputFormat.class);
		
		conf.setBoolean("mapred.output.compress", false);
		FileInputFormat.setInputPaths(conf, inputpath);
		FileOutputFormat.setOutputPath(conf, outputpath);
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(cachepath);
		
		for (FileStatus s : status) {
			//if (s.getPath().getName().startsWith("means")) {
			//	DistributedCache.addCacheFile(s.getPath().toUri(), conf);
			//	System.err.println("Adding " + s.getPath()+ " to distributed cache");
			//}
		    DistributedCache.addCacheFile(s.getPath().toUri(), conf);
            System.err.println("Adding " + s.getPath()+ " to distributed cache");
		}
		conf.setInt("numClusters", k);
		try {
		    ToolRunner.run(conf, new AstroDriver(), args);
		} catch (Exception e) {}
	}
	
	public static void writeResults(Path inputpath, Path outputpath, Path cachepath, int k, int iter, String[] args) throws IOException {       
	    JobConf conf = new JobConf(AstroDriver.class);
        conf.setJobName("Results Map Reduce Iteration: " + iter);
            
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
            
        conf.setMapperClass(ResultsMapper.class);
        conf.setReducerClass(ResultsReducer.class);
    
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(EMOutputFormat.class);
    
        FileInputFormat.setInputPaths(conf, inputpath);
        FileOutputFormat.setOutputPath(conf, outputpath);
        
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(cachepath);
        
        for (FileStatus s : status) {
            DistributedCache.addCacheFile(s.getPath().toUri(), conf);
            System.err.println("Adding " + s.getPath()+ " to distributed cache");
        }
        conf.setInt("numClusters", k);
        try {
            ToolRunner.run(conf, new AstroDriver(), args);
        } catch (Exception e) {}
	}
	
	// run as
    // hadoop jar gmm.jar emmapreduce.AstroDriver \
    // -libjars Jama-1.0.3.jar \
    // input_path init_path output_path k iters
	public static void main(String args[]) throws IOException {
	    
	    String inp_path = args[2];
        String init_path = args[3];
        String out_path = args[4];
        int k = Integer.parseInt(args[5]);
        int numiter = Integer.parseInt(args[6]);
	    
        //Path dictionarypath = new Path("hdfs:///user/hyrkas/kmeans/dictionary.txt");
        //Path inputpath = new Path("hdfs:///user/hyrkas/astro_experiments/input/astro_sample_hadoop.csv");
        Path inputpath = new Path(inp_path);
        System.out.println("in main method");        
        
        
        //int numiter = 5;
        for (int i = 1; i <= numiter; i++) {
            
            

            //Path outputpath = new Path("hdfs:///user/hyrkas/kmeans/output/center_iter_"+i);
            //Path cache = i==1 ? new Path("hdfs:///user/hyrkas/astro_experiments/input/rawcomponentshadoop.csv") : 
            //                    new Path("hdfs:///user/hyrkas/astro_experiments/output/iter"+(i-1));
            Path cache =i==1 ? new Path(init_path) : 
                               new Path(out_path + "/iter"+ (i - 1));
            
            Path outputpath = new Path(out_path + "/iter" + i);
            runIter(inputpath, outputpath, cache, k, i, args);
            //if (i == numiter) {
            //    writeResults(inputpath, new Path("hdfs:///user/hyrkas/seaflow_experiments/thompson0_output/iter"+i+"_results"), outputpath, k, i, args);
            //}
            //ArrayList<Cluster> clusters = driver.loadCluster(outputpath);
            //driver.printClusters(clusters);
            //ArrayList<String> distances = driver.loadDistance(outputpath);
            //driver.printDistance(distances);
          
        }
        
        

    }
}
