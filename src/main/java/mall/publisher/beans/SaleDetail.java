package mall.publisher.beans;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/09/10:33
 * @Description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SaleDetail {

    private String order_detail_id;
    private String order_id;
    private String order_status;
    private String create_time;
    private String user_id;
    private String sku_id;
    private String user_gender;
    private Integer user_age;
    private String user_level;
    private Double sku_price;
    private String sku_name;
    private String dt;

    private String  es_metadata_id;
}