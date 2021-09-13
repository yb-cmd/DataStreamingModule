package mall.publisher.services;

import com.alibaba.fastjson.JSONObject;
import mall.publisher.beans.*;

import java.io.IOException;
import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/09/10:58
 * @Description: the interface for different types of queries
 */
public interface PublisherService {

    //DAU
    Integer getDAUByDate(String date);

    Integer getNewMidCountByDate(String date);

    // DAU per hour
    List<DAUPerHour> getDAUPerHourOfDate(String date);

    //GMV
    Double getGMVByDate(String date);

    // GMV per hour
    List<GMVPerHour> getGMVPerHourOfDate(String date);

    //details
    JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException;
}