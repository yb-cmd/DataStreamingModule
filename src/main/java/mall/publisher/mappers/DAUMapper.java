package mall.publisher.mappers;

import com.baomidou.dynamic.datasource.annotation.DS;
import mall.publisher.beans.DAUPerHour;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/09/13:54
 * @Description:
 */
@DS("hbase")
@Repository
public interface DAUMapper {
    Integer getDAUByDate(String date);
    Integer getNewMidCountByDate(String date);
    List<DAUPerHour> getDAUPerHourOfDate(String date);

}
