/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/23/13
 * Time: 6:43 PM
 * To change this template use File | Settings | File Templates.
 */
package com.xingcloud.xa.hbase.filter;

import com.xingcloud.xa.hbase.util.ByteUtils;
import com.xingcloud.xa.hbase.util.HBaseEventUtils;
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterCondition;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;


public class XARowKeyPatternFilter extends FilterBase {
    private static Logger LOG = LoggerFactory.getLogger(XARowKeyFilter.class);

    private List<RowKeyFilterCondition> conditions=null;
    private int conditionIndex=0;
    private boolean filterOutRow = false;
    private Comparator<RowKeyFilterCondition> conditionComparator=new Comparator<RowKeyFilterCondition>() {
        @Override
        public int compare(RowKeyFilterCondition o1, RowKeyFilterCondition o2) {
            if(Bytes.compareTo(o1.getStartRk(),o2.getStartRk())>0)
                return 1;
            else if(Bytes.compareTo(o1.getStartRk(),o2.getStartRk())<0)
                return -1;
            else
                return 0;
        }
    };

    public XARowKeyPatternFilter() {
        super();
    }

    public XARowKeyPatternFilter(List<RowKeyFilterCondition> conditions){
        super();
        this.conditions=new ArrayList<RowKeyFilterCondition>();
        for(RowKeyFilterCondition condition :conditions){
            if(!this.conditions.contains(condition))
                this.conditions.add(condition);
        }
        RowKeyFilterCondition[] sortCondition=this.conditions.toArray(new RowKeyFilterCondition[this.conditions.size()]);
        Arrays.sort(sortCondition,conditionComparator);
        this.conditions=Arrays.asList(sortCondition);
    }



    @Override
    public void reset() {
        this.filterOutRow = false;
    }


    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        //LOG.info("filterKeyValue");
        if (this.filterOutRow) {
                if(this.conditionIndex==this.conditions.size()){
                    LOG.info("filter KeyValue return NEXT_ROW");
                    return ReturnCode.NEXT_ROW;
                }
                LOG.info("filter KeyValue return SEEK_NEXT_USING_HINT");
                return ReturnCode.SEEK_NEXT_USING_HINT;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRowKey(byte[] data, int offset, int length) {
        byte[] rk = Arrays.copyOfRange(data, offset, offset + length);
        //LOG.info("filter RowKey");
        if(conditions!=null){
            if(conditionIndex<conditions.size()&&!conditions.get(conditionIndex).accept(rk)){
                LOG.info("not accept by condition "+conditionIndex);
                return toNextCondition(rk);
            }else {
                //LOG.info("accept by condition "+conditionIndex+
                //        " :"+Bytes.toStringBinary(conditions.get(conditionIndex).getStartRk()));
                this.filterOutRow=false;
                //LOG.info("filter RowKey return false;");
                return this.filterOutRow;
            }
        }
        return this.filterOutRow;
    }

    private boolean toNextCondition(byte[] rk){
       while(conditionIndex<conditions.size()){
           if(conditions.get(conditionIndex).rkCompareTo(rk)<=0)
               break;
           conditionIndex++;
       }
       if(conditionIndex<conditions.size()&&conditions.get(conditionIndex).accept(rk))
       {
           this.filterOutRow=false;
           //LOG.info("to NextCondition return false");
           return this.filterOutRow;
       }else {
           this.filterOutRow=true;
           //LOG.info("to NextCondition return true");
           return this.filterOutRow;
       }
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue kv) {
        LOG.info("getNextKeyHint ");
        byte[] rk = kv.getRow();
        //resetIndex();
        while(conditionIndex<this.conditions.size()){
            LOG.info("conditionIndex "+conditionIndex);
            RowKeyFilterCondition condition=this.conditions.get(conditionIndex);
            if(condition.rkCompareTo(rk)<=0){
                KeyValue newKV = new KeyValue(condition.getDestination(), kv.getFamily(), kv.getQualifier());
                this.filterOutRow=false;
                LOG.info("pattern "+Bytes.toString(condition.getStartRk()));
                LOG.info("rk "+Bytes.toStringBinary(rk));
                LOG.info("bigPattern ");
                LOG.info("conditionIndex "+conditionIndex);
                LOG.info(" skip to "+Bytes.toStringBinary(newKV.getRow()));


                return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                        .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                        .getFamilyLength(), null, 0, 0);
            }
            conditionIndex++;
        }
        byte[] result=increaseFirstByte(this.conditions.get(conditionIndex-1).getEndRk());
        KeyValue newKV=new KeyValue(result,kv.getFamily(),kv.getQualifier());

        LOG.info("increase Result "+Bytes.toString(result));
        LOG.info("conditionIndex "+conditionIndex);

        return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                    .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                    .getFamilyLength(), null, 0, 0);

    }

    public static byte[] increaseFirstByte(byte[] orig){
        byte[] result=new byte[orig.length];
        result[0]=(byte)(orig[0]+1);
        for(int i=1;i<orig.length;i++){
            result[i]=orig[i];
        }
        return result;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        LOG.info("Read fields of XARowKeyConditonFilter...");
        int size = in.readInt();
        LOG.info("Patterns size: " + size);
        List<RowKeyFilterCondition> conditions = new ArrayList<RowKeyFilterCondition>(size);
        for (int i = 0; i < size; i++) {
            String conditionType = new String(Bytes.readByteArray(in));
            LOG.info("condition type " + conditionType);
            try {
                Class conditionClass=Class.forName(conditionType);
                RowKeyFilterCondition condition=(RowKeyFilterCondition)conditionClass.newInstance();
                condition.readFields(in);
                LOG.info(Bytes.toStringBinary(condition.getStartRk()));
                conditions.add(condition);
            } catch (Exception e) {
                e.printStackTrace();
                throw  new IOException(e);
            }
        }
        this.conditions=conditions;
        this.filterOutRow = false;
        this.conditionIndex=0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(conditions.size());
        for (RowKeyFilterCondition condition : conditions) {
            //LOG.info("Write pattern: " + condition);
            condition.write(out);
        }
    }

    private void resetIndex() {
        conditionIndex=0;
    }


}

