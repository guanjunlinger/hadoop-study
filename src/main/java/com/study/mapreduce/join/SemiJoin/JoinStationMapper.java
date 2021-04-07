package com.study.mapreduce.join.SemiJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\\s+");
        context.write(new TextPair(arr[0], "0"), new Text(arr[1]));
    }
}

