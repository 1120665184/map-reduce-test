/*
 * Email:        quyongquan@qq.com
 * FileName:     WordCountReducer
 * CreationDate: 2019/11/27
 * Author:       渠永泉
 */
package top.quyq.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Quyq
 **/

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //求和
        int num = 0;
        for(IntWritable n : values){
            num +=n.get();
        }
        v.set(num);
        //输出
        context.write(key,v);
    }
}
