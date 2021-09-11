package mall.publisher.beans;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/09/10:32
 * @Description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GMVPerHour {

    private  String hour;
    private  Double gmv;
}
