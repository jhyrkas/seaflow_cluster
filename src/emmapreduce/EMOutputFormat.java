package emmapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * Customized output format that writes to different output file based on its key. For example ("outputA", "blahblah")
 * will write "blahblah" to file whose name starts with outputA. The actual key is ignored.
 * @author haijieg
 *
 */
public class EMOutputFormat extends MultipleTextOutputFormat<Text, Text> {
    protected String generateFileNameForKeyValue(Text key, Text value,String name) {
    	return key.toString();
    }

	@Override
	protected Text generateActualKey(Text key, Text value) {
		return null;
	}
}