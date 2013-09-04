package com.xingcloud.xa.hbase.util.rowkeyCondition;

import com.xingcloud.xa.hbase.util.ByteUtils;
import com.xingcloud.xa.hbase.util.Condition;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/4/13
 * Time: 2:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class  RowKeyFilterPattern implements RowKeyFilterCondition {
    private byte[] pattern;
    public RowKeyFilterPattern(){

    }
    public RowKeyFilterPattern(String pattern){
        this.pattern= ByteUtils.toBytesBinary(pattern);
    }

    public void readFields(DataInput in) throws IOException {
        pattern=Bytes.readByteArray(in);
    }
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out,Bytes.toBytes(this.getClass().getName()));
        Bytes.writeByteArray(out,pattern);
    }

    @Override
    public boolean isAccept(byte[] rk) {
        if(Bytes.startsWith(rk, pattern))
            return true;
        return false;
    }
    @Override
    public byte[] getStartRk() {
        return pattern;
    }

    @Override
    public byte[] getEndRk() {
        return ByteUtils.binaryIncrementPos(pattern,1l);
    }


}
