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

package tv.icntv.logsys.config;

import tv.icntv.logsys.xmlObj.XmlLog;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-17
 * Time: 上午11:49
 * To change this template use File | Settings | File Templates.
 */
public class LogConfiguration implements LogConfigurable{
    private XmlLog xmlLog;
    @Override
    public XmlLog getConf() {
        try {
            parserXml();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return xmlLog;
    }

    public void parserXml() throws Exception{
        SAXReader reader = new SAXReader();
        Document document = reader.read(Thread.currentThread().getContextClassLoader().getResourceAsStream(getFileName()));
        Element  root = document.getRootElement();
        String token = root.attributeValue("token");
        String table = root.attributeValue("table");
        String[][] arrRowConfig = new String[root.elements().size()][3];

        for(int i=0;i<root.elements().size();i++){
            Element row = (Element) root.elements().get(i);

            arrRowConfig[i][0] = row.attributeValue("index");
            arrRowConfig[i][1] = row.attributeValue("cf");
            arrRowConfig[i][2] = row.attributeValue("qualifier");
        }
        this.xmlLog = new XmlLog(arrRowConfig,token,table);

    }
    private String fileName;

    public LogConfiguration(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

//    public static void main(String[] args) {
//        LogConfiguration logConfiguration = new LogConfiguration("cdn_log_mapping.xml");
//        XmlLog xmlLog = logConfiguration.getConf();
//        System.out.println("token:"+xmlLog.getToken());
//        String[][] arrConf = xmlLog.getLogToTableMaping();
//        for(int i=0;i<arrConf.length;i++){
//            for(int j=0;j<arrConf[0].length;j++){
//                System.out.print(arrConf[i][j] + "    ");
//            }
//            System.out.println();
//        }
//    }
}
