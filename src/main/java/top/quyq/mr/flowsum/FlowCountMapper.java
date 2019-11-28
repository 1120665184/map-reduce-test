/*
 * Email:        quyongquan@qq.com
 * FileName:     FlowCountMapper
 * CreationDate: 2019/11/28
 * Author:       渠永泉
 */
package top.quyq.mr.flowsum;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Quyq
 **/

public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        k.set(fields[1]);
        v.setUpFlow(Long.parseLong(fields[fields.length - 3]))
                .setDownFlow(Long.parseLong(fields[fields.length - 2]));

        context.write(k,v);

    }
}
