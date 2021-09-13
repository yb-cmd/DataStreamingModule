package mall.publisher.services;
import com.alibaba.fastjson.JSONObject;
import mall.publisher.dao.*;
import mall.publisher.beans.*;
import mall.publisher.mappers.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/09/11:12
 * @Description:
 */
public class Publisher {
    @Service
    public class PublisherServiceImpl implements  PublisherService {

        @Autowired
        private DAUMapper dauMapper;

        @Autowired
        private GMVMapper gmvMapper;

        @Autowired
        private ESDAO esDao;

        @Override
        public Integer getDAUByDate(String date) {

            System.out.println("the DAU by day....");

            return dauMapper.getDAUByDate(date);
        }

        @Override
        public Integer getNewMidCountByDate(String date) {

            System.out.println("the new user by day....");

            return dauMapper.getNewMidCountByDate(date);

        }

        @Override
        public List<DAUPerHour> getDAUPerHourOfDate(String date) {
            System.out.println("the DAU per hour by day....");

            return dauMapper.getDAUPerHourOfDate(date);
        }

        @Override
        public Double getGMVByDate(String date) {
            System.out.println("the GMV by day....");

            return gmvMapper.getGMVByDate(date);
        }

        @Override
        public List<GMVPerHour> getGMVPerHourOfDate(String date) {
            System.out.println("the GMV per hour by day....");

            return gmvMapper.getGMVPerHourOfDate(date);
        }

        @Override
        public JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException {
            System.out.println("ES query result....");

            return esDao.getESData(date,startpage,size,keyword);
        }
    }
}
