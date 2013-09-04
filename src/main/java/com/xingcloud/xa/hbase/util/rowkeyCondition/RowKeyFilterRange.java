package com.xingcloud.xa.hbase.util.rowkeyCondition;

import com.xingcloud.xa.hbase.util.ByteUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/4/13
 * Time: 2:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class RowKeyFilterRange implements RowKeyFilterCondition{
    private byte[] srk;
    private byte[] enk;
    public RowKeyFilterRange(String srk,String enk){
        this.srk= ByteUtils.toBytesBinary(srk);
        this.enk= ByteUtils.toBytesBinary(enk);
    }
    public RowKeyFilterRange(){}
    public void readFields(DataInput in) throws IOException {
        srk=Bytes.readByteArray(in);
        enk=Bytes.readByteArray(in);
    }
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out,Bytes.toBytes(this.getClass().getName()));
        Bytes.writeByteArray(out,srk);
        Bytes.writeByteArray(out,enk);
    }
    public boolean isAccept(byte[] rk){
        if(Bytes.compareTo(rk, srk)>=0&&Bytes.compareTo(rk,enk)<0)
            return true;
        return false;
    }

    @Override
    public byte[] getStartRk() {
        return srk;
    }

    @Override
    public byte[] getEndRk() {
        return ByteUtils.binaryIncrementPos(enk,1l);
    }
}