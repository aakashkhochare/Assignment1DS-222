package mlld.assignment1.naivebayes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.validator.routines.UrlValidator;


public class NBTest {
	public static String[] stopWords ={"a","about","above","after","again","against","all","am","an","and","any","are","arent","as","at","be","because","been","before","being","below","between","both","but","by","cant","cannot","could","couldn't","did","didnt","do","does","doesnt","doing","dont","down","during","each","few","for","from","further","had","hadnt","has","hasnt","have","havent","having","he","hed","hell","hes","her","here","heres","hers","herself","him","himself","his","how","how's","i","id","ill","im","ive","if","in","into","is","isnt","it","its","its","itself","lets","me","more","most","mustnt","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shant","she","shed","shell","shes","should","shouldnt","so","some","such","than","that","thats","the","their","theirs","them","themselves","then","there","theres","these","they","theyd","theyll","theyre","theyve","this","those","through","to","too","under","until","up","very","was","wasnt","we","wed","well","were","weve","were","werent","what","whats","when","whens","where","wheres","which","while","who","whos","whom","why","whys","with","wont","would","wouldnt","you","youd","youll","youre","youve","your","yours","yourself","yourselves"};
	public static Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopWords));
	public static void main(String[] args) {
		File f = new File(args[0]);
		File f1 = new File(args[1]);
		String line = "";
		Long classSum = 0l;
		Map<String,Long> classMap = new HashMap<String,Long>();
		Map<String, Long> conditionalMap = new HashMap<String, Long>();
		Map<String, Long> numberofTerms = new HashMap<String, Long>();
		Long vocabulary = 0l;
		long countSucc=0;
		long countTot=0;
        try {
			BufferedReader b = new BufferedReader(new FileReader(f));
			String[] schemes = {"http","https"};
            UrlValidator defaultValidator = new UrlValidator(schemes);
			while ((line = b.readLine()) != null) {
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
			
			b = new BufferedReader(new FileReader(f1));
			while ((line = b.readLine()) != null) {
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
					countSucc++;
				}
				countTot++;
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println(countSucc + " " + countTot);
        System.out.println((double)countSucc/countTot);
	}
}
