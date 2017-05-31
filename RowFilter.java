package org.myorg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RowFilter{
  public static class Map1 extends Mapper<LongWritable,Text,Text,Text>  {
              public void map(LongWritable k, Text v, Context con) throws IOException, InterruptedException{
                           String line=v.toString();
                           String[] words=line.split("|");
                           String company=words[0];
                           String product=words[1];
                           if(company.contains("NA")||product.contains("NA"))
                                   con.write(v, new Text());
              }
  }
  public static void main(String[] args) throws Exception  {
              Configuration c=new Configuration();
              String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
              Path InputPath=new Path(files[0]);
              Path OutputPath=new Path(files[1]);
              Job j = new Job(c,"RowFilter");
              j.setJarByClass(RowFilter.class);
              j.setMapperClass(Map1.class);
              j.setNumReduceTasks(0);
              j.setOutputKeyClass(Text.class);
              j.setOutputValueClass(Text.class);
              FileInputFormat.addInputPath(j,InputPath);
               FileOutputFormat.setOutputPath(j, OutputPath);
                System.exit(j.waitForCompletion(true) ? 0:1);
             
  }

}