package mall.publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/09/10:33
 * @Description:
 */
@Data
@AllArgsConstructor
public class Stat {
    String title;
    List<mall.publisher.beans.Option> options;
}
