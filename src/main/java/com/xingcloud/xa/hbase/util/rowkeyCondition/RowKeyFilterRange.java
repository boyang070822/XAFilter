package com.xingcloud.xa.hbase.util.rowkeyCondition;

import com.xingcloud.xa.hbase.util.ByteUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static Logger logger= LoggerFactory.getLogger(RowKeyFilterRange.class);
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
        //logger.info("srk "+Bytes.toStringBinary(srk));
        //logger.info("enk "+Bytes.toStringBinary(enk));
    }
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out,Bytes.toBytes(this.getClass().getName()));
        Bytes.writeByteArray(out,srk);
        Bytes.writeByteArray(out,enk);
    }
    public int accept(byte[] rk){

        if(Bytes.compareTo(rk, srk)>=0&&Bytes.compareTo(rk,enk)<0){
            //logger.info(Bytes.toStringBinary(rk)+" :"+" "+Bytes.toStringBinary(srk)+", "+Bytes.toStringBinary(enk));
            return 0;
        }
        //logger.info("not accept "+Bytes.toStringBinary(rk)+"  "+Bytes.toStringBinary(srk)+"---"+Bytes.toStringBinary(enk));
        if(Bytes.compareTo(rk,srk)<0)
            return -1;
        return 1;

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