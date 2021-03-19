package com.study.mapreduce.sort;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class SecondarySort {
    /**
     * goods_id   click_num
     * 默认只有一个Reduce Task
     */
    public static class IntPair implements WritableComparable<IntPair> {
        int first;
        int second;

        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }

        @Override
        public int compareTo(IntPair o) {
            if (first != o.first)
                return first < o.first ? 1 : -1;
            else if (second != o.second)
                return second < o.second ? -1 : 1;
            else
                return 0;
        }

        @Override
        public int hashCode() {
            return first * 157 + second;
        }

        @Override
        public boolean equals(Object right) {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else
                return false;
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {

        @Override
        public int getPartition(IntPair key, IntWritable value, int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int l = ip1.getFirst();
            int r = ip2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        private final IntPair intPair = new IntPair();
        private final IntWritable intWritable = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int left;
            int right = 0;
            if (tokenizer.hasMoreTokens()) {
                left = Integer.parseInt(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens())
                    right = Integer.parseInt(tokenizer.nextToken());
                intPair.set(right, left);
                intWritable.set(left);
                context.write(intPair, intWritable);
            }
        }
    }

    public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable> {
        private final Text left = new Text();

        public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            left.set(Integer.toString(key.getFirst()));
            for (IntWritable val : values)
                context.write(left, val);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SecondarySort");
        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setPartitionerClass(FirstPartitioner.class);

        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String[] otherArgs = new String[]{
                "hdfs://localhost:9000/mr/in/goods_visit2",
                "hdfs://localhost:9000/mr/out/secondarysort/goods_visit2"
        };
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}