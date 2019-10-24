package cs435.hadoop;

import java.util.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;

public class ProfileBReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private Map<String, Double> unigramValue = new TreeMap<>();       //key: unigram \t documentID, value: TF-IDF 
    private Map<String, Double> sentenceValue = new TreeMap<>();      //key: count \t sentence, value: sum TF-IDF
    
    private Map<String, Integer> test = new TreeMap<>();


    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String article = "";
        unigramValue.clear();
        
        for(Text val : values){
            //It's from cache mapper
            if(val.charAt(0) == 'C'){
                String input = val.toString().substring(1);
                String[] data = input.split(":");    //unigram:TF-IDF
                String mapKey = data[0];
                double mapValue = Double.parseDouble(data[1]);
                unigramValue.put(mapKey, mapValue);
            } else {
                //It's from documents mapper
                article = val.toString().substring(1);
            }
        }

        //Split by '. '
        String[] splitByPeriod = article.split("\\. ");
        int count = 0;
        
        for(String sentence : splitByPeriod){
            //Go through words and remove non-alphanumeric characters
            String[] words = sentence.split("\\s+");
            Set<String> unigramsInSentence = new HashSet<>();
            
            //Go through unique words and find top 5 tf-idf values
            PriorityQueue<Double> topValuesForSentence = new PriorityQueue<>();
            for(String word : words){
                word = word.toLowerCase().replaceAll("[^A-Za-z0-9]\\s","");

                if(word.length() > 0 && !topValuesForSentence.contains(word)){
                
                    try{
                        double tf_idfValue = unigramValue.get(word);
                        addValue(topValuesForSentence, tf_idfValue);
                    } catch (NullPointerException e){
                    }

                }
            }
            
            for(String unigram : test.keySet()){
                context.write(new IntWritable(test.get(unigram)), new Text(unigram));
            }
            
            //Calculate sum(top 5 tf-idf values)
            double totalValueForSentence = 0.0;
            Iterator<Double> itr = topValuesForSentence.iterator();
            while(itr.hasNext()){
                totalValueForSentence += itr.next();
            }
            
            //Add index to sentence to keep order
            sentence = Integer.toString(count) + "\t" + sentence;
            count++;
            
            //Add sentence and value to map
            sentenceValue.put(sentence, totalValueForSentence);
        }
        
        //Sort sentence map by values
        LinkedHashMap<String, Double> sentencesSortedByValue = new LinkedHashMap<>();
         sentenceValue.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sentencesSortedByValue.put(x.getKey(), x.getValue()));
         
         //Get top 3 values
         ArrayList<String> top3Sentences = new ArrayList<>();
         int top3 = 0;
         for(String sentence : sentencesSortedByValue.keySet()){
            if(top3 < 3){
                top3Sentences.add(sentence);
                top3++;
            }
         }
         
         Collections.sort(top3Sentences);
         
         //Put sentences together
         String summary = "";
         for(String sentence : top3Sentences){
            summary += sentence.substring(2) + ".";
         }   
         
         context.write(key, new Text(summary));
    }
    
    private boolean isInvalidLine(String line){
        return line == null || line.length() == 0;
    }
    
    private void addValue(PriorityQueue<Double> topValuesForSentence, double currentVal){
        topValuesForSentence.add(currentVal);
        
        if(topValuesForSentence.size() > 5){
            topValuesForSentence.poll();   
        }
    }
}
