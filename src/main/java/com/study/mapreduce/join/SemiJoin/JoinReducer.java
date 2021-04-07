package com.study.mapreduce.join.SemiJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

    public void reduce(TextPair key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text stationName = new Text(values.next());
        while (values.hasNext()) {
            Text record = values.next();
            output.collect(stationName, record);
        }
    }
}

