/* Copyright 2013 Future TV, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package tv.icntv.logsys.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-6
 * Time: 下午5:46
 */
public class HbaseUtils {
//    private Configuration configuration = null;
    private static HTablePool hTablePool = null;
    private static int poolSize = 500;

    public synchronized static HbaseUtils getHbaseUtils(Configuration configuration) {
        return new HbaseUtils(configuration);
    }

    private HbaseUtils(Configuration configuration) {
//        this.configuration = configuration;
        hTablePool = new HTablePool(configuration, poolSize);
    }

    public HTable getHtable(String table) {
        HTable hTable = (HTable) hTablePool.getTable(Bytes.toBytes(table));
        hTable.setAutoFlush(false);
        hTable.setScannerCaching(1000);
        return hTable;
    }

    public  void release(HTable hTable) {
        if (null != hTable) {
            try {
                hTable.flushCommits();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } finally {
                    hTablePool.putTable(hTable);
            }
        }
    }


    public static void main(String[]args) throws MalformedURLException {
//        String url="http://movie.douban.com/subject/3094909/";
//        System.out.println(TableUtil.reverseUrl("http://so.letv.com/film/78222.html"));
       final HbaseUtils h=HbaseUtils.getHbaseUtils(HBaseConfiguration.create());
        HTable table= h.getHtable("leixw_douban_webpage");
        Scan scan=new Scan();

        try {
            ResultScanner resultScanner=table.getScanner(scan);
            for (Result r : resultScanner) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));

            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            h.release(table);
        }
        Pattern p= Pattern.compile("http://t.iqiyi.com/m/[0-9]+");
        System.out.println(p.matcher("http://t.iqiyi.com/m/1013032").find());
    }
}
