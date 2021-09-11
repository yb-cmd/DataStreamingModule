package mall.canal.logger;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import mall.common.constants.MyConstants;
/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/11/15:37
 * @Description: pretty much the same with MyCanalClient, but added with the method sending to Kakfa
 */
@SuppressWarnings("ALL")
public class MyCanalClientToKafka {
    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {

        /*
            SocketAddress address,: canal server's host and port.
                        canal.ip
                        canal.port
            String destination: pick which data source it came from and the according properties'
                         canal.destinations = example,example2,example3
            Change these properties accordingly.
         */
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop103", 11111), "example", null, null);

        canalConnector.connect();

        // GMV which sql database it's from
        canalConnector.subscribe("210422test.order_info");
        //pulling data
        while(true){

            //if there is no data at the moment，set Message's id = -1
            Message message = canalConnector.get(100);

            if (message.getId() == -1){

                System.out.println("no data at the moment, waiting");

                Thread.sleep(5000);

                continue;

            }


            // create a list to store the data
            List<CanalEntry.Entry> entries = message.getEntries();

            for (CanalEntry.Entry entry : entries) {
                // get table header and name
                String tableName = entry.getHeader().getTableName();
                if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA){
                    // see if the sql content is insert or what
                    parseData(tableName,entry.getStoreValue());

                }
            }
        }
    }

    //tableName's needed
    private static void parseData(String tableName, ByteString storeValue) throws InvalidProtocolBufferException {

        //deserialization to RowChange
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

        /*
               for the GMV's related table order_info, most likely it contains the data that shouldn't be changed
               hence only need to filter for insert as GMV is counting the total amount of newly added orders

         */
        // order_info : insert
        if (tableName.equals("order_info") && rowChange.getEventType() == CanalEntry.EventType.INSERT){

            sendDataToKafka(MyConstants.GMALL_ORDER_INFO,rowChange);

            // order_detail : insert
        }else if (tableName.equals("order_detail") && rowChange.getEventType() == CanalEntry.EventType.INSERT){

            sendDataToKafka(MyConstants.GMALL_ORDER_DETAIL,rowChange);
            // user_info:  insert | update
        }else if(tableName.equals("user_info") && ( rowChange.getEventType() == CanalEntry.EventType.INSERT || rowChange.getEventType() == CanalEntry.EventType.UPDATE )){

            sendDataToKafka(MyConstants.GMALL_USER_INFO,rowChange);

        }

    }
    private  static  void sendDataToKafka(String topic, CanalEntry.RowChange rowChange){

        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDatasList) {

            JSONObject jsonObject = new JSONObject();

            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            for (CanalEntry.Column column : afterColumnsList) {

                jsonObject.put(column.getName(),column.getValue());

            }
            //to avoid them arrive at the sametime
            int delaySecond = new Random().nextInt(5);

            MyProducer.sendRecord(topic,jsonObject.toJSONString());

        }
    }

}
