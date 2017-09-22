package mlld.assignment1.naiveBayesTrain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.validator.routines.*;


public class NBTrain {
	private static String FILENAME;
	public static String[] stopWords ={"a","about","above","after","again","against","all","am","an","and","any","are","arent","as","at","be","because","been","before","being","below","between","both","but","by","cant","cannot","could","couldn't","did","didnt","do","does","doesnt","doing","dont","down","during","each","few","for","from","further","had","hadnt","has","hasnt","have","havent","having","he","hed","hell","hes","her","here","heres","hers","herself","him","himself","his","how","how's","i","id","ill","im","ive","if","in","into","is","isnt","it","its","its","itself","lets","me","more","most","mustnt","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shant","she","shed","shell","shes","should","shouldnt","so","some","such","than","that","thats","the","their","theirs","them","themselves","then","there","theres","these","they","theyd","theyll","theyre","theyve","this","those","through","to","too","under","until","up","very","was","wasnt","we","wed","well","were","weve","were","werent","what","whats","when","whens","where","wheres","which","while","who","whos","whom","why","whys","with","wont","would","wouldnt","you","youd","youll","youre","youve","your","yours","yourself","yourselves"};
	public static Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopWords));
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<String, Long> classCount = new HashMap<String, Long>();  
		Long defaultClassCount = new Long(0l);
		//Map<Pair, Long> conditionalCount= new HashMap<Pair,Long>();
		Map<String, Long> conditionalCount= new HashMap<String,Long>();
		Map<String, Long> termCount = new HashMap<String, Long>();
		Set<String> vocabulary = new HashSet<String>();
		BufferedWriter bw = null;
		FileWriter fw = null;
		try {

            File f = new File(args[0]);

            BufferedReader b = new BufferedReader(new FileReader(f));

            String readLine = "";

            //System.out.println("Reading file using Buffered Reader");
            String[] schemes = {"http","https"};
            UrlValidator defaultValidator = new UrlValidator(schemes);
            while ((readLine = b.readLine()) != null) {
                //System.out.println(readLine);
                String parts[ ] = readLine.split("\t");
                //System.out.println(parts[0] + "---------------" +parts[1]);
                
                String classnames[] = parts[0].split(",");
                for(String classname : classnames){
                	defaultClassCount++;
                	if(classCount.get(classname.trim())!=null){
                		classCount.put(classname.trim(), classCount.get(classname.trim())+1);
                	}
                	else{
                		//System.out.println(readLine);
                		//System.out.println(classname.trim());
                		classCount.put(classname.trim(), 1l);
                	}
                }
                for(String term : parts[1].replaceAll("( )+", " ").replaceAll("<", "").replaceAll(">", "").split(" ")){
                	if (!defaultValidator.isValid(term)){
                		term = term.replaceAll("\\p{P}", "").toLowerCase().trim();
                		if(!term.equals("") && !stopWordSet.contains(term)){
                			vocabulary.add(term);
                			for(String classname : classnames){
                				//Pair p = new Pair(classname.trim(), term);
                				String p = classname.trim() + "@" + term;
                				if(conditionalCount.get(p)!=null){
                					conditionalCount.put(p,conditionalCount.get(p)+1l);
                				}
                				else{
                					conditionalCount.put(p, 1l);
                				}
                				if(termCount.get(classname.trim()) != null){
                					termCount.put(classname.trim(), termCount.get(classname.trim())+1l);
                				}
                				else{
                					termCount.put(classname.trim(), 1l);
                				}
                			}
                		}
                	}
                	else{
                		//System.out.println(term);
                	}
                }
            }
            
    		
    		FILENAME = args[1];
			fw = new FileWriter(FILENAME);
			bw = new BufferedWriter(fw);
			
            for(String p : conditionalCount.keySet()){
            	//System.out.println("3@"+p + "\t" +Long.toString(conditionalCount.get(p)));
            	bw.write("3@"+p+ "\t" +Long.toString(conditionalCount.get(p)) + "\n");
            }
            
            
            for(String s : termCount.keySet()){
            	//System.out.println("4@"+s + " " +Long.toString(termCount.get(s)));
            	bw.write("4@"+s + "\t" +Long.toString(termCount.get(s)) + "\n");
            	//System.out.println((double)classCount.get(s)/defaultClassCount);
            }
            
            //System.out.println("5@vocabulary\t" + vocabulary.size());
            bw.write("5@vocabulary\t" + vocabulary.size() + "\n");
            
            System.out.println("Classcount " + classCount.keySet().size());
            bw.write("1@classSum\t" + defaultClassCount + "\n");
            
            Long l = 0l;
            for(String s : classCount.keySet()){
            	l = l + (classCount.get(s));
            	//System.out.println("2@"+s + "\t"+ Long.toString(classCount.get(s)));
            	bw.write("2@"+s + "\t"+ Long.toString(classCount.get(s)) + "\n");
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }finally {

			try {

				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
        }
	}
}
