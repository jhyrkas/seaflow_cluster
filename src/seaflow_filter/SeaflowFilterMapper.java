package seaflow_filter;


import java.io.IOException;

import seaflow.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SeaflowFilterMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
    
    private static int CHL_INDEX = 3;
    private static double CHL_THRESH  = 10000.0;
    
    @Override
    public void map(LongWritable id, Text line,
            OutputCollector<IntWritable, Text> out, Reporter reporter)
            throws IOException {
        
        // easiest thing is for astronomy data to be transformed into opp format:
        // file no, line no, num dimensions, measurements....
    	try {
    		OPP opp = new OPP(line.toString());
    		if (opp.getMeasurements()[CHL_INDEX] < CHL_THRESH) {
                return;
            }
            
            
            out.collect(new IntWritable(0), line);
    	} catch (Exception e) {
    		System.out.println("Exception encountered in OPP creation");
    	}
        
        
    }
    
}
