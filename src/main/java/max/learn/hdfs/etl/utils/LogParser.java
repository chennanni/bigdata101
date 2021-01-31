package max.learn.hdfs.etl.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class LogParser {

    private Logger logger = Logger.getLogger(LogParser.class);

    public Map<String, String> parse(String log)  {

        Map<String, String> logInfo = new HashMap<String,String>();
        IPParser ipParse = IPParser.getInstance();
        if(StringUtils.isNotBlank(log)) {

            String[] splits = log.split(" ");
            String ip = splits[0];
            String method = splits[1];
            String url = splits[2];

            logInfo.put("ip",ip);
            logInfo.put("method",method);
            logInfo.put("url",url);

            IPParser.RegionInfo regionInfo = ipParse.analyseIp(ip);
            logInfo.put("country",regionInfo.getCountry());
            logInfo.put("province",regionInfo.getProvince());
            logInfo.put("city",regionInfo.getCity());

            logger.debug("parse: " + ip + url + method + url);

        } else{
            logger.error("日志记录的格式不正确：" + log);
        }

        return logInfo;
    }

}