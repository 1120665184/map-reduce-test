/*
 * Email:        quyongquan@qq.com
 * FileName:     WordCount
 * CreationDate: 2019/11/27
 * Author:       渠永泉
 */
package top.quyq.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Quyq
 **/

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置jar加载路径
        job.setJarByClass(WordCount.class);
        //设置mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置inputFormat,如果不设置默认使用TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设为4M
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        //设置文件输入，输出位置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
