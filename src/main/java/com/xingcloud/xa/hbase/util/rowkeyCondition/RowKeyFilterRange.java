package com.xingcloud.xa.hbase.util.rowkeyCondition;

import com.xingcloud.xa.hbase.util.ByteUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/4/13
 * Time: 2:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class RowKeyFilterRange implements RowKeyFilterCondition, Comparable<RowKeyFilterCondition>{
    public static Logger logger= LoggerFactory.getLogger(RowKeyFilterRange.class);
    private byte[] srk;
    private byte[] enk;
    private byte[] tailSrt=null;
    private byte[] tailEnd=null;
    private boolean sampling=false;
    private byte[] destination;
    private int tailLen;
    public RowKeyFilterRange(String srk,String enk){
        this.srk= ByteUtils.toBytesBinary(srk);
        this.enk= ByteUtils.toBytesBinary(enk);
        this.tailSrt=new byte[]{0};
        this.tailEnd=new byte[]{-1};
        tailLen=tailSrt.length;
        this.destination=this.srk;
        this.sampling=false;
    }
    public RowKeyFilterRange(String srk,String enk,String tailStart,String tailEnd){
        this.srk=Bytes.toBytesBinary(srk);
        this.enk=Bytes.toBytesBinary(enk);
        this.tailSrt=Bytes.toBytesBinary(tailStart);
        this.tailEnd=Bytes.toBytesBinary(tailEnd);
        assert (this.tailSrt.length==this.tailEnd.length);
        this.destination=this.srk;
        this.sampling=true;
        tailLen=tailSrt.length;
    }
    public RowKeyFilterRange(){}
    public void readFields(DataInput in) throws IOException {
        srk=Bytes.readByteArray(in);
        enk=Bytes.readByteArray(in);
        tailSrt=Bytes.readByteArray(in);
        tailEnd=Bytes.readByteArray(in);
        this.destination=this.srk;
        int i=0;
        while(i<tailSrt.length){
            if(!(tailSrt[i]==(byte)0&&tailEnd[i]==(byte)-1)){
                sampling=true;
                break;
            }
            i++;
        }
        tailLen=tailSrt.length;
        //logger.info("srk "+Bytes.toStringBinary(srk));
        //logger.info("enk "+Bytes.toStringBinary(enk));
    }
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out,Bytes.toBytes(this.getClass().getName()));
        Bytes.writeByteArray(out,srk);
        Bytes.writeByteArray(out,enk);
        Bytes.writeByteArray(out,tailSrt);
        Bytes.writeByteArray(out,tailEnd);
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof RowKeyFilterRange){
            RowKeyFilterRange refRange=(RowKeyFilterRange)o;
            if(Bytes.equals(refRange.getStartRk(),srk)&& Bytes.equals(refRange.getEndRk(), this.getEndRk()))
                return true;
        }
        return false;
    }

    public boolean accept(byte[] rk){

        if(Bytes.compareTo(rk, srk)>=0&&Bytes.compareTo(rk,enk)<0){
            if(!sampling)
                return true;
            byte[] rkTail= Arrays.copyOfRange(rk,rk.length-tailLen,rk.length);
            //byte[] rkTailEnd= Arrays.copyOfRange(rk,rk.length-tailEnd.length,rk.length);
            if(Bytes.compareTo(rkTail,tailSrt)>=0&&Bytes.compareTo(rkTail,tailEnd)<0)
                return true;
            //logger.info(Bytes.toStringBinary(rk)+" :"+" "+Bytes.toStringBinary(srk)+", "+Bytes.toStringBinary(enk));
            //return 0;
        }
        return false;

    }

    public int rkCompareTo(byte[] rk){
        if(Bytes.compareTo(rk, srk)>=0&&Bytes.compareTo(rk,enk)<0){
            int tailOffset=rk.length-tailLen;
            byte[] rkHead=Arrays.copyOf(rk,tailOffset);
            rkHead[tailOffset-1]+=1;
            destination=Bytes.add(rkHead,tailSrt);
            return 0;
        }
        //logger.info("not accept "+Bytes.toStringBinary(rk)+"  "+Bytes.toStringBinary(srk)+"---"+Bytes.toStringBinary(enk));
        if(Bytes.compareTo(rk,srk)<0){
            return -1;
        }
        return  1;
    }

    @Override
    public byte[] getStartRk() {
        return srk;
    }

    @Override
    public byte[] getDestination(){
        return destination;
    }

    @Override
    public byte[] getEndRk() {
        return ByteUtils.binaryIncrementPos(enk,1l);
    }

    @Override
    public int compareTo(RowKeyFilterCondition o) {
        return Bytes.compareTo(this.getStartRk(),o.getStartRk());
    }
}