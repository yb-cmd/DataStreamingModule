package mall.publisher.dao;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/09/11:17
 * @Description: the interface for es queries
 */
public interface ESDAO {
    JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException;
}
