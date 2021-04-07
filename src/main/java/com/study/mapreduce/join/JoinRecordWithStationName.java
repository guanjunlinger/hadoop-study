package com.study.mapreduce.join;

import org.apache.hadoop.mapreduce.Job;

public class JoinRecordWithStationName {

    public static void main(String[] args) {

        Job job = Job.getInstance("join");
        job.setJarByClass(JoinRecordWithStationName.class);

        Path recordInputPath = new Path(args[0]);//天气记录数据源，这里是牵扯到多路径输入和多路径输出的问题。默认是从args[0]开始
        Path stationInputPath = new Path(args[1]);//气象站数据源
        Path outputPath = new Path(args[2]);//输出路径


        MultipleInputs.addInputPath(job,recordInputPath,TextInputFormat.class,JoinRecordMapper.class);//读取天气记录Mapper
        MultipleInputs.addInputPath(job,stationInputPath,TextInputFormat.class,JoinStationMapper.class);//读取气象站Mapper
        FileOutputFormat.setOutputPath(job,outputPath);
        job.setReducerClass(JoinReducer.class);// Reducer
        job.setNumReduceTasks(2);

        job.setPartitionerClass(KeyPartitioner.class);//自定义分区
        job.setGroupingComparatorClass(GroupingComparator.class);//自定义分组

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true)?0:1;
    }
}
