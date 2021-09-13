package mall.publisher.mappers;

import com.baomidou.dynamic.datasource.annotation.DS;
import mall.publisher.beans.GMVPerHour;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/09/13:57
 * @Description:
 */
@DS("mysql")
@Repository
public interface GMVMapper {

    // query gmv by date
    Double getGMVByDate(String date);

    // query gmv in one day in time period
    List<GMVPerHour> getGMVPerHourOfDate(String date);

}
