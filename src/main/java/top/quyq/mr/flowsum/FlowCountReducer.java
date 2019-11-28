/*
 * Email:        quyongquan@qq.com
 * FileName:     FlowCountReducer
 * CreationDate: 2019/11/28
 * Author:       渠永泉
 */
package top.quyq.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Quyq
 * @TODO
 **/

public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    FlowBean bean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long upCount = 0;
        long downCount = 0;

        for (FlowBean b : values){
            upCount += b.getUpFlow();
            downCount += b.getDownFlow();
        }
        bean.setUpFlow(upCount)
                .setDownFlow(downCount)
                .setSumFlow(upCount + downCount);

        context.write(key,bean);
    }
}
