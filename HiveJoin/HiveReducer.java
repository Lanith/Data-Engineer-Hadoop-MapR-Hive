 import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class HiveReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      
       //Variables to aid the join process
       private String pageId,age;   
           // Reducer begins below
       public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
       {
    	 
        while (values.hasNext())
        {
             String currValue = values.next().toString();		 
             String valueSplitted[] = currValue.split("~");
             /*identifying the record source that corresponds to a cell number
             and parses the values accordingly*/
             if(valueSplitted[0].equals("P"))
             {
               pageId=valueSplitted[1].trim();
             }
             else if(valueSplitted[0].equals("U"))
             {
              //getting the page_id and age
            	 age = valueSplitted[1].trim();
            		   
             }
            
        }
        
        //send final output to file
        if((pageId!=null && age!=null) )
        {	
        	            output.collect(new Text(pageId), new Text(age));
        }
        else if(pageId==null){
               output.collect(new Text("pageId"), new Text(age));
        }
        else if(age==null){
               output.collect(new Text(pageId), new Text("age"));
        }
        
      }}
      
      
       