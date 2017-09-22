package mlld.assignment1.naivebayes;


import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class NBTrain {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{
    String[] schemes = {"http","https"};
    UrlValidator defaultValidator = new UrlValidator(schemes);
    public static String[] stopWords ={"a","about","above","after","again","against","all","am","an","and","any","are","arent","as","at","be","because","been","before","being","below","between","both","but","by","cant","cannot","could","couldn't","did","didnt","do","does","doesnt","doing","dont","down","during","each","few","for","from","further","had","hadnt","has","hasnt","have","havent","having","he","hed","hell","hes","her","here","heres","hers","herself","him","himself","his","how","how's","i","id","ill","im","ive","if","in","into","is","isnt","it","its","its","itself","lets","me","more","most","mustnt","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shant","she","shed","shell","shes","should","shouldnt","so","some","such","than","that","thats","the","their","theirs","them","themselves","then","there","theres","these","they","theyd","theyll","theyre","theyve","this","those","through","to","too","under","until","up","very","was","wasnt","we","wed","well","were","weve","were","werent","what","whats","when","whens","where","wheres","which","while","who","whos","whom","why","whys","with","wont","would","wouldnt","you","youd","youll","youre","youve","your","yours","yourself","yourselves"};
	public static Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopWords));
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String line = value.toString();
    	/*Split the line on tab*/
        String parts[ ] = line.split("\t");        
        String classnames[] = parts[0].split(",");
        for(String classname : classnames){
        	context.write(new Text("1@"+"any"), new LongWritable(1l));
        	context.write(new Text("2@"+classname.trim()), new LongWritable(1l));
        }
        for(String term : parts[1].replaceAll("( )+", " ").replaceAll("<", "").replaceAll(">", "").split(" ")){
        	if (!defaultValidator.isValid(term)){
        		term = term.replaceAll("\\p{P}", "");
        		term = term.toLowerCase().trim();
        		if(!term.equals("") && !stopWordSet.contains(term)){
        			for(String classname : classnames){
        				context.write(new Text("3@"+classname.trim()+"@"+term), new LongWritable(1l));
        				context.write(new Text("4@"+classname.trim()), new LongWritable(1l));
        			}
        			context.write(new Text("5@"+term), new LongWritable(1l));
        		}
        	}
        }
    }
  }

  public static class IntSumReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
   long vocabulary = 0l;
   public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	   String line = key.toString();
	   String parts[] = line.split("@");
	   long classSum = 0l;
	   long individualClassSum = 0l;
	   long individualTermSum = 0l;
	   long termSum = 0l;
	   if(Integer.parseInt(parts[0]) == 1){
		   for(LongWritable value : values){
			   classSum++;
		   }
		   context.write(new Text("1@classSum "), new LongWritable(classSum));
	   }
	   else if(Integer.parseInt(parts[0]) == 2){
		   individualClassSum = 0l;
		   for(LongWritable value : values){
			   individualClassSum++;
		   }
		   context.write(new Text("2@"+parts[1]), new LongWritable(individualClassSum));
	   }
	   else if(Integer.parseInt(parts[0]) == 3){
		   individualTermSum = 0l;
		   for(LongWritable value : values){
			   individualTermSum++;
		   }
		   context.write(new Text("3@"+parts[1] + "@" + parts[2]), new LongWritable(individualTermSum));
	   }
	   else if(Integer.parseInt(parts[0]) == 4){
		   termSum = 0l;
		   for(LongWritable value : values){
			   termSum++;
		   }
		   context.write(new Text("4@" + parts[1]), new LongWritable(termSum));
	   }
	   else if(Integer.parseInt(parts[0]) == 5){
		   vocabulary++;
	   }
   }
   
   @Override
   protected void cleanup(Context context) {
     try {
		context.write(new Text("5@vocabulary"), new LongWritable(vocabulary));
	} catch (IOException | InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }

  }



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = Job.getInstance(conf, "NBTrain");
    job.setJarByClass(NBTrain.class);
    job.setMapperClass(TokenizerMapper.class);

    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    
    job.setNumReduceTasks(Integer.parseInt(args[2]));
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}