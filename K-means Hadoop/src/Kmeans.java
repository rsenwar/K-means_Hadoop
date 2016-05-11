
import java.util.*; 

import java.io.BufferedReader;
import java.io.IOException; 
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Kmeans 
{ 
	  static DoubleWritable[][] centers;
	  static final int CLUSTER_SIZE = 20;
	  static int dimension;
	  static boolean start = true;
	  static boolean isJobDone = false;

	  static double dataSize;
	  static int iterationsNumber;
	  static int outputIterations;
	  
	  static final double EPS = 0.1;
	  
	  
	  
  public static class Map extends Mapper<LongWritable ,Text ,IntWritable, DoubleArrayWritable>
  { 

	  @Override
	  protected void setup(Context context) throws IOException, InterruptedException 
	  {
		 
		 if(start)
		 {
		    Configuration conf = context.getConfiguration();
		    FileSystem fs = FileSystem.get(conf); 
		    FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
  		    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fsFileSplit.getPath())));
		  		   

 		    String lineReader;
		    int clusterCounter = 0;

		    
		    //Randomly read points to be initial centers
		    while( (lineReader = br.readLine()) != null && clusterCounter < CLUSTER_SIZE)
		    {
		       initializeCentroids(lineReader, clusterCounter);
			   clusterCounter++;
			   
			   int randomSkip = new Random().nextInt(1000);
			   
			   for (int i = 0; i < randomSkip; i++) 
			   {
				   br.readLine();				
			   }
			   
		    }
		   		  	   
		   start = false;
			
		  }
		  
	  }
	  
       
	  @Override
	  public void map(LongWritable key, Text value, Context context) 
	   throws IOException, InterruptedException 
      { 
		  dataSize++;
		 // System.out.println("Beginnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn " + dataSize);

		  
		  StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
		  tokenizer.nextToken();
 			
		  IntWritable[] point = new IntWritable[dimension];
 			
		  double euclideanDistance = 0;
		  double minimum = Double.MAX_VALUE;
		  
		  int col = 0;
		  int cluster = 0;
		  		  
		  while (tokenizer.hasMoreTokens()) 
	 	  {
			 point[col] = new IntWritable( Integer.parseInt(tokenizer.nextToken()) );
			 col++;
				
	 	  }		
				
		  
 			
		  for (int i = 0; i < centers.length; i++) 
		  {
			 euclideanDistance = 0;
			 
			 for (col = 0; col < dimension; col++)
			 {
 	 			double difference = point[col].get() - centers[i][col].get();
 	 				
 	 			euclideanDistance += Math.pow(difference, 2);
 	 			 	 				
 	 		  }
 				

			  if(minimum > Math.sqrt(euclideanDistance))
			  {
				  minimum = Math.sqrt(euclideanDistance);
				  cluster = i;
				  
			  }
 
		  }

 			context.write(new IntWritable(cluster), new DoubleArrayWritable(point));
 			 			    	     	  
      } 
       
	  
	  private void initializeCentroids(String line, int row)
	  {
		  StringTokenizer tokenizer = new StringTokenizer(line, ",");
		  tokenizer.nextToken(); //Skip the indices
			 
		  if(centers == null)
		  {
	 		 dimension = tokenizer.countTokens();
			 centers = new DoubleWritable[CLUSTER_SIZE][dimension];

		  }
			 
		  int col = 0;
		 
		  while (tokenizer.hasMoreElements()) 
		  {
			  centers[row][col] = new DoubleWritable( Double.parseDouble(tokenizer.nextToken()) );
				
			  System.out.print(centers[row][col] + " ---- ");
			  col++;
				
		  } 
			
			System.out.println();
		  
	  }
	  
	  
   } 
   
   
   
  public static class Reduce extends Reducer< IntWritable, DoubleArrayWritable, IntWritable, Text > 
  {  
	  
	  @Override
      public void reduce(IntWritable key, Iterable <DoubleArrayWritable> values, Context context)
    		  throws IOException, InterruptedException 
      { 
    	  

    	 int counter = 0;
	 	   	 
	   	 DoubleWritable[] newCentroid = new DoubleWritable[dimension];
	   	 
	 	   	 
	   	 int dimensionCounter = 0;
	   	 double sum[] = new double[dimension];
	   	 
	   	for (DoubleArrayWritable val : values )
	   	{
   			counter++;
	   		 
	   		Writable[] writable = val.get();	
	   		dimensionCounter = 0;	   		 

		   	while(dimensionCounter < dimension)
	   		{
	   			sum[dimensionCounter] += ( (IntWritable) writable[dimensionCounter] ).get();
	   			dimensionCounter++;
	   		}
	   		
	   	 }
	   	
	   
	   	 dimensionCounter = 0;
	     while(dimensionCounter < dimension)
  		 {
	    	double dimensionCentroid = sum[dimensionCounter]/counter;
	   		newCentroid[dimensionCounter] = new DoubleWritable(dimensionCentroid);
	   		
	   		dimensionCounter++;
	   		
  		 }
	   	 
   	 
	   	 boolean isCentroidsStable = true;
	   	  
	   	 for (int i = 0; i < newCentroid.length; i++) 
	   	 {
	   		 if( Math.abs(centers[key.get()][i].get() - newCentroid[i].get()) > EPS )
	   			 isCentroidsStable = false;
	   			 
	   			 
	   		 centers[key.get()][i] = new DoubleWritable(newCentroid[i].get());  
				   		 
	   	 }
	   	 
	   	 if(isCentroidsStable)
	   	 {
	   		 isJobDone = true;
	   		 outputIterations++;
	   	 
	   	 }else
	   	 {
	   		 isJobDone = false;
	   		 outputIterations--;
	   	 }
	   	 
	   	 
	   	 if(outputIterations == CLUSTER_SIZE){
		   	 context.write(key, new Text( iterationsNumber + "" ));
		   	 System.out.println("iterationsNumber ------> " + iterationsNumber);

		   	 
		 }
  	  
	   	  
//	   	 for (int i = 0; i < cachedPoint.size(); i++) 
//  	 {
//		      context.write(key, new Text(cachedPoint.get(i).toString()));
//
//  	 }
	   	
	   	 
	   	 System.out.println("Data size -----> " + dataSize);
	   	 context.write(key, new Text( (double)(counter / dataSize) * 100 + " %" ));
	   	 context.write(new IntWritable(iterationsNumber), new Text("") );

	      	  
            
      } 
	   
   }  
  
  
  public static class DoubleArrayWritable extends ArrayWritable 
  {
	  
	  public DoubleArrayWritable()
	  {
		  super(IntWritable.class);
	  }

	  
	  public DoubleArrayWritable(Writable[] valueClass) 
	  {
		super(IntWritable.class, valueClass);
	  }
	  
	  public DoubleArrayWritable(DoubleWritable[] valueClass) 
	  {
		super(IntWritable.class, valueClass);
	  }

	    
	    
	  @Override
	  public String toString() 
	  {
	     Writable[] values = get();
	        
	     String strings = "";
	     for (int i = 0; i < values.length; i++) 
	     {
	         strings = strings + " "+ ((IntWritable) values[i]).toString();
	     }
	        
	     return strings;	   
	      
	  }

	   
	}
  
  
   
   //Main function 
   public static void main(String args[])throws Exception 
   {    
	  // Path inputFile = new Path("input_dir/input.txt");
	   Path inputFile = new Path("input_dir/USCensus1990.data.txt");

	   Path outputFile = new Path("output_dir/output");
	   

	   long start, end;
	   
	   start = new Date().getTime();

	   while(!isJobDone) 
	   {

		   //conf.set("mapreduce.output.textoutputformat.separator",":");
		   Configuration conf = new Configuration();
		   dataSize = 0;
		   outputIterations = 0;

		   Job job = new Job(conf, "Clustering");
		   		   
		   job.setJarByClass(Kmeans.class);
		   job.setOutputKeyClass(IntWritable.class);
		   job.setOutputValueClass(DoubleArrayWritable.class);
		    
		   job.setMapperClass(Map.class);
		   job.setReducerClass(Reduce.class);
		    
		   job.setInputFormatClass(TextInputFormat.class);
		   job.setOutputFormatClass(TextOutputFormat.class);
		    
		   FileInputFormat.addInputPath(job, inputFile);
		   FileSystem.get(conf).delete(outputFile,true);
		   FileOutputFormat.setOutputPath(job, outputFile);
		    
		   job.waitForCompletion(true);
		   
		   iterationsNumber++;
		   
	   }
	   
		end = new Date().getTime();
		
	   System.out.println("Job took "+ (end - start)/1000 + " Seconds");
     
   } 
} 

