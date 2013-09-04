package com.xingcloud.xa.hbase.util.rowkeyCondition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/4/13
 * Time: 2:06 PM
 * To change this template use File | Settings | File Templates.
 */
public interface RowKeyFilterCondition {
    public boolean isAccept(byte[] rk);
    public byte[] getStartRk();
    public byte[] getEndRk();
    public void readFields(DataInput in) throws IOException;
    public void write(DataOutput out) throws IOException;
}
