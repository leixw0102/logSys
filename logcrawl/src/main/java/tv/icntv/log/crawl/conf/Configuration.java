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

package tv.icntv.log.crawl.conf;

import com.google.common.base.Preconditions;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

import java.net.URL;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-10
 * Time: 下午2:52
 * To change this template use File | Settings | File Templates.
 */
public class Configuration {
    private static WeakHashMap<String,String> weakRefer=new WeakHashMap<String, String>();
    private static final Set<String> confs=new HashSet<String>();
    private static ClassLoader classLoader=null;

    /**
     * instance configurations
     * @return
     */
    public static Configuration getInstanceConfig(){
        return new Configuration();
    }

    /**
     * constructor
     */
    private Configuration() {
    }

    static {
        classLoader=Thread.currentThread().getContextClassLoader();
        if(null == classLoader){
            classLoader=Configuration.class.getClassLoader();
        }
        addDefaultResource("ftp-crawl-default.xml");
        addDefaultResource("ftp-crawl.xml");
    }

    /**
     *
     * @param key
     * @return
     */
    protected String get(String key){
        return get(key,null);
    }
    protected String get(String key,String defaultValue) {
        String value=weakRefer.get(key);
        if(null == value || value.equals("")){
            return defaultValue;
        }
        return value;
    }
    protected Integer getInt(String key){
        return getInt(key, 0);
    }
    protected Integer getInt(String key,int defaultValue) {

        return Integer.parseInt(get(key,defaultValue+""));
    }
    protected long getLong(String key){
        return getLong(key, 0);
    }
    protected long getLong(String key,long defaultValue) {

        return Long.getLong(get(key,defaultValue+""));
    }
    private synchronized static void addDefaultResource(String name) {
        if(!confs.contains(name)){
            confs.add(name);
            loadResource(name);
        }
    }

    protected boolean getBoolean(String key){
        return getBoolean(key,false);
    }
    protected boolean getBoolean(String key,boolean defaultBoolean){

        return Boolean.parseBoolean(get(key,defaultBoolean+""));
    }
    private static void loadResource(Object name) {
        try {
            DocumentBuilderFactory docBuilderFactory
                    = DocumentBuilderFactory.newInstance();
            //ignore all comments inside the xml file
            docBuilderFactory.setIgnoringComments(true);

            //allow includes in the xml file
            docBuilderFactory.setNamespaceAware(true);
            try {
                docBuilderFactory.setXIncludeAware(true);
            } catch (UnsupportedOperationException e) {

            }
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = null;
            Element root = null;

            if (name instanceof URL) {                  // an URL resource
                URL url = (URL)name;
                doc = builder.parse(url.toString());
            } else if(name instanceof String){
                String config=(String)name;
              doc=  builder.parse(classLoader.getResourceAsStream(config)) ;
            }
            if(null == root){
               root=doc.getDocumentElement();
            }
            NodeList props=root.getChildNodes();
            for(int i=0;i<props.getLength();i++){
                Node propNode = props.item(i) ;
                if(!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element)propNode;
                if ("configuration".equals(prop.getTagName())) {
                    loadResource( prop);
                    continue;
                }
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element)fieldNode;
                    if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
                        attr = ((Text)field.getFirstChild()).getData().trim();
                    }
                    if ("value".equals(field.getTagName()) && field.hasChildNodes())  {
                        value = ((Text)field.getFirstChild()).getData();
                    }
                    if (attr != null) {
                        if (value != null) {
                            weakRefer.put(attr,value);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (DOMException e) {
            throw new RuntimeException(e);
        } catch (SAXException e) {
            throw new RuntimeException(e);
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
    public void iterator(){
       Preconditions.checkArgument(null!= weakRefer && !weakRefer.isEmpty(),"null");
        Set<Map.Entry<String,String>> set=weakRefer.entrySet();
        for(Iterator<Map.Entry<String,String>> it = set.iterator();it.hasNext();){
            Map.Entry<String,String> entry=it.next();
            System.out.println("key="+entry.getKey() +" \t value="+entry.getValue());
        }

    }
    public  static  void main(String[] args){
          Configuration config=new Configuration();
          config.iterator();
    }
}
