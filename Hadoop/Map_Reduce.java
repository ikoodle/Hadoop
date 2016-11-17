/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.j2ee.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

 public class Map_Reduce {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{     
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String child = value.toString().split(" ")[0];
    	String parent = value.toString().split(" ")[1]; 
        context.write(new Text(child), new Text("-" + parent)); 
    	context.write(new Text(parent), new Text("+" + child));
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,  Context context
                       ) throws IOException, InterruptedException {
    	ArrayList<Text> grandparent = new ArrayList<Text>();
    	ArrayList<Text> grandchild = new ArrayList<Text>();
    	for (Text t : values) {
    		String s = t.toString();
    		if (s.startsWith("-")) { 
    			grandparent.add(new Text(s.substring(1)));
    			} else { 
    				grandchild.add(new Text(s.substring(1)));
    			}
                   }
    		 for (int i = 0; i < grandchild.size(); i++){
    		 for (int j = 0; j < grandparent.size(); j++){
    			 context.write(grandchild.get(i), grandparent.get(j));
    		 }
    	}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf);
    job.setJarByClass(Map_Reduce.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
