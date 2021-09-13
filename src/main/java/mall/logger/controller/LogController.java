package mall.logger.controller;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import mall.common.constants.MyConstants;

import lombok.extern.log4j.Log4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * For educational purposes only
 *
 * @Author: T.REN
 * @Date: 2021/09/06/16:06
 * @Description: used to receive the logs send by clients, based on the json logs.
 *               data are stored in Kafka.
 */
@RestController
@Log4j
public class LogController {
    @Autowired
    private KafkaTemplate producer;
    // to assign the url to: http://localhost:8888/mall_logger/applog
    @RequestMapping(value="/applog")
    public Object handleLog(String param){

        log.info(param);


        JSONObject jsonObject = JSON.parseObject(param);

        // depends on the json file to send the contents in unified format
        if (jsonObject.getString("start") != null){

            producer.send(MyConstants.STARTUP_LOG, param );

        }

        if (jsonObject.getString("actions") != null){

            producer.send(MyConstants.ACTIONS_LOG, param );

        }

        if (jsonObject.getString("displays") != null){

            producer.send(MyConstants.DISPLAY_LOG, param );

        }

        if (jsonObject.getString("err") != null){

            producer.send(MyConstants.ERROR_LOG, param );
        }

        if (jsonObject.getString("page") != null){
            producer.send(MyConstants.PAGE_LOG, param );
        }

        return "success";
    }

}
