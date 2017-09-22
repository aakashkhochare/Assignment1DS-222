package mlld.assignment1.naivebayes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class NBTest {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{
    
	Long classSum = 0l;
	Map<String,Long> classMap = new HashMap<String,Long>();
	Map<String, Long> conditionalMap = new HashMap<String, Long>();
	Map<String, Long> numberofTerms = new HashMap<String, Long>();
	Long vocabulary = 0l;
	
	public static String[] stopWords ={"a","about","above","after","again","against","all","am","an","and","any","are","arent","as","at","be","because","been","before","being","below","between","both","but","by","cant","cannot","could","couldn't","did","didnt","do","does","doesnt","doing","dont","down","during","each","few","for","from","further","had","hadnt","has","hasnt","have","havent","having","he","hed","hell","hes","her","here","heres","hers","herself","him","himself","his","how","how's","i","id","ill","im","ive","if","in","into","is","isnt","it","its","its","itself","lets","me","more","most","mustnt","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shant","she","shed","shell","shes","should","shouldnt","so","some","such","than","that","thats","the","their","theirs","them","themselves","then","there","theres","these","they","theyd","theyll","theyre","theyve","this","those","through","to","too","under","until","up","very","was","wasnt","we","wed","well","were","weve","were","werent","what","whats","when","whens","where","wheres","which","while","who","whos","whom","why","whys","with","wont","would","wouldnt","you","youd","youll","youre","youve","your","yours","yourself","yourselves"};
	public static Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopWords));
	String[] schemes = {"http","https"};
    UrlValidator defaultValidator = new UrlValidator(schemes);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String line = value.toString();
    	String parts[] = line.split("\t");
		Map<String,Double> probs = new HashMap<String,Double>();
		for(String term : parts[1].replaceAll("( )+", " ").replaceAll("<", "").replaceAll(">", "").split(" ")){
        	if (!defaultValidator.isValid(term)){
        		term = term.replaceAll("\\p{P}", "");
        		term = term.toLowerCase().trim();
        		if(!term.equals("") && !stopWordSet.contains(term)){
        			for(String classname : classMap.keySet()){
        				double val = 0l;
        				if(conditionalMap.get(classname+"@"+term) != null)
        					val = (double)conditionalMap.get(classname+"@"+term);
        				double prob =(val + 1.0)/(numberofTerms.get(classname) + vocabulary);
        				if(probs.get(classname) == null){
        					probs.put(classname, prob);
        				}
        				else{
        					probs.put(classname, prob * probs.get(classname));
        				}
        			}
        		}
        	}
		}
		double maxProb = 0.0;
		String predict = "";
		for(String classname : probs.keySet()){
			double finalProb = probs.get(classname) * ((double)classMap.get(classname)/classSum);
			probs.put(classname, finalProb);
			if(finalProb > maxProb){
				maxProb = probs.get(classname);
				predict = classname; 
			}
			//System.out.println(classname + " " + probs.get(classname));
		}
		String partsTemp [] = parts[0].split(",");
		for(int i = 0 ; i < partsTemp.length; i++){
			partsTemp[i] = partsTemp[i].trim();
		}
		Set<String> mySet = new HashSet<String>(Arrays.asList(partsTemp));
		if(mySet.contains(predict)){
			context.write(value, new LongWritable(1l));
		}
		else{
			context.write(value, new LongWritable(0));
		}
    }
    
    @Override
    protected void setup(Context context){
    	FileSystem fs;
    	Configuration configuration = new Configuration();
    	configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	
		configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		//configuration.addResource(new Path("/home/disthadoop/hadoop/hadoop-2.7.3/etc/hadoop/core-site.xml"));
		//configuration.addResource(new Path("/home/disthadoop/hadoop/hadoop-2.7.3/etc/hadoop/hdfs-site.xml"));
		try {
			fs = FileSystem.get(new URI("hdfs://turing.cds.iisc.ac.in:8020"),configuration);
			//fs = FileSystem.get(configuration);
			String filePath = context.getConfiguration().get("filePath");
			
			FileStatus[] status = fs.listStatus(new Path(filePath));
			BufferedReader br;
			
			for(int i = 0 ; i < status.length; i++){
				
				br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line;
				while ((line = br.readLine()) != null) {
					String parts[] = line.split("\t");
					String leftParts[] = parts[0].split("@");
					if(Integer.parseInt(leftParts[0]) == 1){
						classSum = Long.parseLong(parts[1]);
					}
					else if(Integer.parseInt(leftParts[0]) == 2){
						classMap.put(leftParts[1].trim(), Long.parseLong(parts[1]));
					}
					else if(Integer.parseInt(leftParts[0]) == 3){
						String p = leftParts[1] + "@" + leftParts[2];
						conditionalMap.put(p.trim(), Long.parseLong(parts[1]));
					}
					else if(Integer.parseInt(leftParts[0]) == 4){
						numberofTerms.put(leftParts[1].trim(), Long.parseLong(parts[1]));
					}
					else if(Integer.parseInt(leftParts[0]) == 5){
						vocabulary = vocabulary + Long.parseLong(parts[1]);
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
  }

  public static class IntSumReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
   long countSucc = 0;
   long countFail = 0;
   public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	   for(LongWritable value: values){
		   if(value.get() == 0l){
			   countFail++;
		   }
		   else if(value.get() == 1l){
			   countSucc++;
		   }
	   }
   }
   
   @Override
   protected void cleanup(Context context) {
     try {
		context.write(new Text("Success "), new LongWritable(countSucc));
		context.write(new Text("Failure "), new LongWritable(countFail));
	} catch (IOException | InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   
  }
  
  public static class Mapper2 extends Mapper<Object, Text, Text, Text>{
	  public void map(Object key, Text value, Context context){
		  String parts[] = value.toString().split("\t");
		  if(parts[0].trim().equals("Success")){
			  try {
				context.write(new Text("Success"), new Text(parts[1]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		  else if(parts[0].trim().equals("Failure")){
			  try {
					context.write(new Text("Failure"), new Text(parts[1]));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		  }
  	  }
  }
  
  public static class Reducer2 extends Reducer<Text, Text,Text, Text> {
	  long countSucc = 0l;
      long countFail = 0l;	
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  if(key.toString().equals("Success")){
			  for(Text value: values){
				  countSucc += Long.parseLong(value.toString());
			  }
		  }
		  else if(key.toString().equals("Failure")){
			  for(Text value: values){
				  countFail += Long.parseLong(value.toString());
			  }
		  }
	   }
	   
	   @Override
	   protected void cleanup(Context context) {
	     try {
			context.write(new Text("Success"), new Text(Long.toString(countSucc)));
			context.write(new Text("Total"), new Text(Long.toString(countFail+countSucc)));
			context.write(new Text("Accuracy"), new Text(Double.toString((double) countSucc / (countFail+countSucc))));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	   }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    conf.set("filePath", args[1]);
    Job job = Job.getInstance(conf, "NBTest");
    job.setJarByClass(NBTest.class);
    job.setMapperClass(TokenizerMapper.class);

    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    
    job.setNumReduceTasks(Integer.parseInt(args[4]));
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();	
    
    Job job2 = Job.getInstance(conf2, "NBTest");
    job2.setJarByClass(NBTest.class);
    job2.setMapperClass(Mapper2.class);
    //job.setCombinerClass(Reducer1.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class); 
    job2.setNumReduceTasks(1);  
    FileInputFormat.addInputPath(job2, new Path(args[2]));
    FileOutputFormat.setOutputPath(job2, new Path(args[3]));
    
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}