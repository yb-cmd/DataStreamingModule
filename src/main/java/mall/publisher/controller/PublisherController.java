package mall.publisher.controller;

import com.alibaba.fastjson.JSONObject;
import mall.publisher.beans.DAUPerHour;
import mall.publisher.beans.GMVPerHour;
import mall.publisher.services.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/09/10:56
 * @Description: Unfinished
 */
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping(value = "/realtime-total")
    public Object handle1(String date){

        Integer dauToday = publisherService.getDAUByDate(date);
        Integer newMidCount = publisherService.getNewMidCountByDate(date);
        Double gmvByDate = publisherService.getGMVByDate(date);

        ArrayList<JSONObject> result = new ArrayList<>();

        JSONObject jsonObject1 = new JSONObject();
        JSONObject jsonObject2 = new JSONObject();
        JSONObject jsonObject3 = new JSONObject();

        jsonObject1.put("id","dau");
        jsonObject1.put("name","DAU");
        jsonObject1.put("value",dauToday);

        jsonObject2.put("id","new_mid");
        jsonObject2.put("name","Daily Signups");
        jsonObject2.put("value",newMidCount);

        jsonObject3.put("id","order_amount");
        jsonObject3.put("name","GMV");
        jsonObject3.put("value",gmvByDate);

        result.add(jsonObject1);
        result.add(jsonObject2);
        result.add(jsonObject3);

        return result;

    }

    @RequestMapping(value = "/realtime-hours")
    public Object handle2(String date,String id){

        // date
        String yesTodayStr = LocalDate.parse(date).minusDays(1).toString();

        JSONObject result = new JSONObject();

        if (id.equals("dau")){
            List<DAUPerHour> todayData = publisherService.getDAUPerHourOfDate(date);
            List<DAUPerHour> yesTodayData = publisherService.getDAUPerHourOfDate(yesTodayStr);

            result.put("yesterday",handleDAUData(yesTodayData));
            result.put("today",handleDAUData(todayData));

        }else  if (id.equals("order_amount")){

            List<GMVPerHour> todayGMVData = publisherService.getGMVPerHourOfDate(date);
            List<GMVPerHour> yesTodayGMVData = publisherService.getGMVPerHourOfDate(yesTodayStr);

            result.put("today",handleGMVData(todayGMVData));
            result.put("yesterday",handleGMVData(yesTodayGMVData));

        }

        return result;

    }

    @RequestMapping(value = "/sale_detail")
    public JSONObject getESData(String date,Integer startpage,Integer size,String keyword) throws IOException {

        return  publisherService.getESData(date,startpage,size,keyword);

    }

    public JSONObject  handleDAUData(List<DAUPerHour> todayData){
        JSONObject result = new JSONObject();

        for (DAUPerHour data : todayData) {

            result.put(data.getHour(),data.getDau());

        }

        return result;

    }

    public JSONObject  handleGMVData(List<GMVPerHour> todayData){
        JSONObject result = new JSONObject();

        for (GMVPerHour data : todayData) {

            result.put(data.getHour(),data.getGmv());

        }

        return result;

    }

}
