package tv.icntv.log.stb.epgoperate;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 14-12-25
 * Time: 上午10:21
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    public static void main(String[] args){
        String aa = "URL=http://sns.is.ysten.com/CNTV/index.html?action=detail&object=985766";
        System.out.println(aa.substring(aa.indexOf("action=detail&object=")+21));
    }
}
