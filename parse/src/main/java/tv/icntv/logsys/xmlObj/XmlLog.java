package tv.icntv.logsys.xmlObj;

/**
 * Created with IntelliJ IDEA.
 * User: wangliang
 * Date: 13-10-18
 * Time: 上午10:08
 */
public class XmlLog {

    public XmlLog(String[][] logToTableMaping,String token,String table){
        this.token = token;
        this.table = table;
        this.logToTableMaping =  logToTableMaping;
    }
    private String token;//日志文件分隔符
    private String table;//数据库表名
    private String[][] logToTableMaping; //日志文件字段与数据库字段对应关系

    public String getToken() {
        return token;
    }

    public String getTable() {
        return table;
    }

    public String[][] getLogToTableMaping(){
        return this.logToTableMaping;
    }
}
