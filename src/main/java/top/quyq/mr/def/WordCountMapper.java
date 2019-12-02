/*
 * Email:        quyongquan@qq.com
 * FileName:     WordCountMapper
 * CreationDate: 2019/11/27
 * Author:       渠永泉
 */
package top.quyq.mr.def;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 类泛型：
 * KEYIN:输入数据key
 * VALUEIN：输入数据value
 * KEYOUT:输出数据key
 * VALUEOUT：输出数据value
 * @author Quyq
 * map
 **/

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    IntWritable v = new IntWritable(1);
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for(String work : value.toString().split(" ")){
            k.set(work);
            context.write(k,v);
        }
    }
}
