package acadgild.session3.assignment2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * FilterMapper to filter the records where Company or Product name is not
 * available.
 *
 */
public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] array = value.toString().split("[|]");
		Text myKey = new Text(key.toString());
		String invalidColumnVal = "NA";

		Text company = new Text(array[0]);
		Text product = new Text(array[1]);

		if (!(invalidColumnVal.equalsIgnoreCase(company.toString()))
				|| !(invalidColumnVal.equalsIgnoreCase(product.toString()))) {
			context.write(myKey, value);
		}
	}

}