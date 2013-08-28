package com.xingcloud.xa.hbase.filter;

import com.xingcloud.xa.hbase.util.ByteUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
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
 * Date: 8/28/13
 * Time: 9:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class XARkConditionFilter extends FilterBase {
    public  static final Logger logger= LoggerFactory.getLogger(XARkConditionFilter.class);

    private RowKeyRange condition;
    private Filter filter;
    public XARkConditionFilter(){
        super();
    }
    public XARkConditionFilter(RowKeyRange condition,Filter filter){
           this.condition=condition;
           this.filter=filter;
    }
    public XARkConditionFilter(String srk,String enk,Filter filter){
        this.condition=new RowKeyRange(srk,enk);
        this.filter=filter;
    }
    public XARkConditionFilter(byte[] srk,byte[] enk,Filter filter){
        this.condition=new RowKeyRange(srk,enk);
        this.filter=filter;
    }


    @Override
    public void reset() {
        filter.reset();
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
         byte[] rk= Arrays.copyOfRange(buffer,offset,offset+length);
         if(condition.inRange(rk)!=0){
             logger.info("condition not hit. filterRowKey Return false.");
             return false;
         }
         logger.info("condition hit. filterRowKey apply filter.");
         boolean result=filter.filterRowKey(buffer,offset,length);
         logger.info("filter(filterRowKey) result "+result);
         return result;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        byte[] rk=kv.getRow();
        if(condition.inRange(rk)!=0){
            logger.info("filterKeyValue: condition not hit");
            logger.info("return ReturnCode.INCLUDE");
            return ReturnCode.INCLUDE;
        }
        logger.info("condition hit. filterKeyValue apply filter");
        ReturnCode result=filter.filterKeyValue(kv);
        logger.info("filter(filterKeyValue) result "+result);
        return result;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue kv) {
        logger.info("getNextKeyHint ");
        KeyValue result=filter.getNextKeyHint(kv);
        logger.info("getNextKeyHint "+result);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //out.writeInt(condition.getStartRk().length);
        Bytes.writeByteArray(out,condition.getStartRk());
        //out.writeInt(condition.getEndRk().length);
        Bytes.writeByteArray(out,condition.getEndRk());
        String typeName=filter.getClass().getName();
        logger.info("typeName "+typeName);
        System.out.println("typeName "+typeName);
        Bytes.writeByteArray(out, ByteUtils.toBytesBinary(typeName));
        filter.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        logger.info("read srk");
        byte[] srk=Bytes.readByteArray(in);
        logger.info("read srk "+ByteUtils.toStringBinary(srk));
        logger.info("read enk");
        byte[] enk=Bytes.readByteArray(in);
        logger.info("read enk "+ByteUtils.toStringBinary(enk));
        this.condition=new RowKeyRange(srk,enk);
        logger.info("read type name ");
        byte[] typeBytes=Bytes.readByteArray(in);
        String filterClassName=ByteUtils.toStringBinary(typeBytes);
        logger.info("filterClassName "+filterClassName);
        try {
            Class filterClass=Class.forName(filterClassName);
            try {
                this.filter=(Filter)filterClass.newInstance();
                this.filter.readFields(in);
            } catch (InstantiationException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (IllegalAccessException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    public static class RowKeyRange {
        private byte[] startRk;
        private byte[] endRk;
        public RowKeyRange(String startRowKey,String endRowKey){
            this.startRk= ByteUtils.toBytesBinary(startRowKey);
            this.endRk= ByteUtils.toBytesBinary(endRowKey);
        }
        public RowKeyRange(byte[] srk,byte[] enk){
            this.startRk=srk;
            this.endRk=enk;
        }
        public int inRange(byte[] rk){
            if(Bytes.compareTo(rk, startRk)<0)
                return -1;
            else if(Bytes.compareTo(rk,endRk)>0)
                return 1;
            else
                return 0;
        }
        public byte[] getStartRk(){
            return startRk;
        }
        public byte[] getEndRk(){
            return endRk;
        }
    }
}
