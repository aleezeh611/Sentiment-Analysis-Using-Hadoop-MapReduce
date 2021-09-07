
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files

public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	    private Text word = new Text();
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	      Text temptxt = new Text(value.toString());
	      if(temptxt.find("iBooks") != -1) {							//check if keyword exists in line only then run map function
	    	  String positivebag = new String();
	    	  String negativebag = new String();
	    	
	    	  try {
	    	      File myObj = new File("positive.txt");				//read all words from positive and negative text files in two strings separately
	    	      Scanner myReader = new Scanner(myObj);
	    	      while (myReader.hasNextLine()) {
	    	        positivebag += myReader.nextLine();
	    	        positivebag += " ";
	    	      }
	    	      myReader.close();
	    	    } catch (FileNotFoundException e) {
	    	      System.out.println("An error occurred.");
	    	      e.printStackTrace();
	    	    }
	    	  try {
	    	      File myObj2 = new File("negative.txt");
	    	      Scanner myReader2 = new Scanner(myObj2);
	    	      while (myReader2.hasNextLine()) {
	    	        negativebag += myReader2.nextLine();
	    	        negativebag += " ";
	    	      }
	    	      myReader2.close();
	    	    } catch (FileNotFoundException e) {
	    	      System.out.println("An error occurred.");
	    	      e.printStackTrace();
	    	    }
	    	  
	    	  IntWritable result = new IntWritable(-1);
	    	  StringTokenizer itr = new StringTokenizer(value.toString());
	    	  
	    	  int stoploop = 0;																//will be used to stop the loop once the comment has ended
	    	  int poscount = 0;
	    	  int negcount = 0;
	    	  while (itr.hasMoreTokens()) {
	    		  word.set(itr.nextToken());
	    		  if (word.find("Text=") != -1) {											//only begin actual work when the comment starts
	    			  while (itr.hasMoreTokens()) {
	    				  String str = new String(word.toString());
	    				  str = str.replaceAll("[^a-zA-Z0-9]", "");							//remove all special characters from each word to properly compare with bag of words
	    				  word.set(str);
	    				  StringTokenizer positr = new StringTokenizer(positivebag);		//tokenize both strings that have stored positive and negative words
	    		    	  StringTokenizer negitr = new StringTokenizer(negativebag);
	    		    	  while(positr.hasMoreTokens()) {									//in a loop compare each word with words in bag of words and keep count
	    		    		  String currentword = new String(positr.nextToken());
	    		    		  if (currentword.equals(word.toString())) {
		    					  poscount+=1;
	    		    		  }	
	    		    	  }
	    		    	  while(negitr.hasMoreTokens()) {
	    		    		  String currentword = new String(negitr.nextToken());
	    		    		  if (currentword.equals(word.toString())){
		    					  negcount+=1;
	    		    		  }	
	    		    	  }
	    				  word.set(itr.nextToken());
	    				  if(word.find(Character.toString('"')) != -1 ) {
	    					  stoploop = 1;
	    					  break;
	    				  }
	    			  }
	    			  if(stoploop == 1) {
	    				  word.set("key");												//send the same key so all the data is grouped under same key and reducer function can work on all the data that it needs
	    				  if (poscount >= negcount) {									//for each comment mapper will see if specific comment is pos or neg
	    					  result.set(1);											//send 0 for neg comment and 1 for pos comment
	    				  }
	    				  else {
	    					  result.set(0);
	    				  }
	    				  context.write(word, result);									//reducer will then see if majority comments are positive or negative
	    				  break;
	    			  }
	    		  }
	    	  }
	      }
	     }
}
