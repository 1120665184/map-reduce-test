/*
 * Email:        quyongquan@qq.com
 * FileName:     FlowBean
 * CreationDate: 2019/11/28
 * Author:       渠永泉
 */
package top.quyq.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Quyq
 * Bean
 **/

public class FlowBean implements Writable {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //跟序列化方法顺序必须一致
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public FlowBean setUpFlow(long upFlow) {
        this.upFlow = upFlow;
        return this;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public FlowBean setDownFlow(long downFlow) {
        this.downFlow = downFlow;
        return this;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public FlowBean setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
        return this;
    }

    @Override
    public String toString() {
        return "upFlow=" + upFlow +
                "\t downFlow=" + downFlow +
                "\t sumFlow=" + sumFlow ;
    }
}
