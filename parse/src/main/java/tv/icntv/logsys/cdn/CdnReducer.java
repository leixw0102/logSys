package tv.icntv.logsys.cdn;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import tv.icntv.logsys.config.LogConfigurationFactory;
import tv.icntv.logsys.reducer.IcntvReducer;
import tv.icntv.logsys.utils.GenerateId;
import tv.icntv.logsys.utils.HbaseUtils;
import tv.icntv.logsys.xmlObj.XmlLog;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

/**
 * Created with IntelliJ IDEA.
 * User: wangliang
 * Date: 13-10-23
 * Time: 下午3:03
 */
public class CdnReducer extends IcntvReducer<Text,Text,ImmutableBytesWritable>{
//    private static HbaseUtils hbaseUtils=null;
//    private HTable htable=null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        hbaseUtils= HbaseUtils.getHbaseUtils(context.getConfiguration());
//
//        String table=context.getJobName();
//        System.out.println("....."+table.replace("icntv_",""));
//        htable=hbaseUtils.getHtable(table.replace("icntv_",""));
//        System.out.println(null == htable);
                //super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        if(null != htable){
//            try{
//            htable.flushCommits();
//            }catch (Exception e){
//                e.printStackTrace();
//            }finally {
//               hbaseUtils.release(htable);
//            }
//        }
        //super.cleanup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void parser(Object key, Iterable values, XmlLog configuration, Context context) {
        Text key1= (Text) key;
        String[][] rowConf = configuration.getLogToTableMaping();
        String[] arrResultRow = new String[rowConf.length];

        String[] arrRow = null;
        Long longBeginDateTime = 0L;
        Long longEndDateTime = 0L;

        int rowNum=0;
        Long longDateTimeTemp = 0L;

        Long byteTemp = 0L;


        for(Iterator<Text> it=values.iterator();it.hasNext();){
            //合并
            Text text = it.next();
            arrRow = text.toString().split("</>");

            //解析日期
            String strDateTime = arrRow[1];
            DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.UK);
            Date dt2 = null;
            try {
                dt2 = format.parse(strDateTime);
                longDateTimeTemp = dt2.getTime();
            } catch (ParseException e) { e.printStackTrace();}

            //获取分组中最大和最小日期
            if(rowNum==0){
                arrResultRow[1] = strDateTime;
                longBeginDateTime = longDateTimeTemp;
                arrResultRow[2] = strDateTime;
                longEndDateTime = longDateTimeTemp;
            }else{
                if(longDateTimeTemp < longBeginDateTime){
                    arrResultRow[1] = strDateTime;
                    longBeginDateTime = longDateTimeTemp;
                }
                if(longDateTimeTemp > longEndDateTime){
                    arrResultRow[2] = strDateTime;
                    longEndDateTime = longDateTimeTemp;
                }
            }

            arrResultRow[0] = arrRow[0];//IP地址
            arrResultRow[3] = arrRow[2];//uri地址
            arrResultRow[4] = arrRow[3];//status状态
            byteTemp += Long.parseLong(arrRow[4]);//
            arrResultRow[5] = String.valueOf(byteTemp);//流量
            arrResultRow[6] = arrRow[5];//useragent
            rowNum++;
        }


        Put put = new Put(Bytes.toBytes((key1.toString()+ GenerateId.generateIdSuffix()))) ;
        for(int i=0;i<rowConf.length;i++){
            int index = Integer.parseInt(rowConf[i][0]);
            if(index>arrResultRow.length){
                break;
            }
            put.add(rowConf[i][1].getBytes(),rowConf[i][2].getBytes(),arrResultRow[index-1].getBytes());
        }

        try {
            context.write(new ImmutableBytesWritable(put.getRow()), put) ;
//             htable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public XmlLog getConf() {
        return  LogConfigurationFactory.getLogConfigurableInstance("tv.icntv.logsys.config.LogConfiguration", "cdn_log_mapping.xml").getConf();
    }

}
