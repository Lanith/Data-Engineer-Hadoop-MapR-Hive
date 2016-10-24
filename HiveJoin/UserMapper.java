import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class UserMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
{
	private IntWritable outkey = new IntWritable();
	private Text outvalue = new Text();
	private static final Logger logger = Logger.getLogger(UserMapper.class);	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException{
		try{
		String[] strArrText = value.toString().split("\t");
		//outkey-value/field used for table join
		String strValue = "U~" + strArrText[1];// value to be displayed in the joined table
		
		output.collect(new Text(strArrText[0]), new Text(strValue));
		
		}
		catch(NumberFormatException e){
			System.out.println("Not a valid number");
			System.out.println(e.getMessage());
		}
	}

}