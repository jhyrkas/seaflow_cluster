package mrresults;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ResultsReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>{
    public void reduce(IntWritable key, Iterator<Text> values,
            OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
        
        //pass through as CSV
        while (values.hasNext()) {
            Text line = values.next();
            out.collect(new Text("cluster" + key.get()), new Text(line.toString() + "," + key.get()));
        }
    }
}
