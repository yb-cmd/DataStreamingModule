package mall.publisher.dao;
import com.alibaba.fastjson.JSONObject;
import mall.publisher.beans.*;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: REN
 * @Date: 2021/09/09/11:19
 * @Description: for the es query example ,check the notes
 */
@Repository
public class ESDAOImpl implements mall.publisher.dao.ESDAO {
    @Autowired
    private JestClient jestClient;

    @Override
    public JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException {

        SearchResult searchResult = query(date, startpage, size, keyword);

        List<SaleDetail> detail = getDetail(searchResult);

        Stat ageStat = parseAgeStat(searchResult);
        Stat genderStat = parseGenderStat(searchResult);

        ArrayList<Stat> stats = new ArrayList<>();

        stats.add(ageStat);
        stats.add(genderStat);

        JSONObject jsonObject = new JSONObject();

        jsonObject.put("total",searchResult.getTotal());
        jsonObject.put("stats",stats);
        jsonObject.put("detail",detail);

        return jsonObject;
    }



    public SearchResult query(String date, Integer startpage, Integer size, String keyword) throws IOException {

        //indexing, change accordingly
        String indexName= "gmall2021_sale_detail" + date;

        int from = (startpage - 1) * size;

        //match
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword);

        //aggs
        TermsBuilder termsBuilder1 = AggregationBuilders.terms("gendercount").field("user_gender").size(2);
        TermsBuilder termsBuilder2 = AggregationBuilders.terms("agecount").field("user_age").size(150);

        //query
        String querySource = new SearchSourceBuilder().query(matchQueryBuilder).aggregation(termsBuilder1).aggregation(termsBuilder2).from(from).size(size).toString();

        Search search = new Search.Builder(querySource).addType("_doc").addIndex(indexName).build();

        SearchResult searchResult = jestClient.execute(search);


        return searchResult;
    }


    public List<SaleDetail> getDetail( SearchResult searchResult){

        List<SaleDetail> result =new ArrayList<>();

        List<SearchResult.Hit<SaleDetail, Void>> hits = searchResult.getHits(SaleDetail.class);

        for (SearchResult.Hit<SaleDetail, Void> hit : hits) {

            SaleDetail saleDetail = hit.source;

            saleDetail.setEs_metadata_id(hit.id);

            result.add(saleDetail);

        }
        return result;

    }
    /*
     *   to get the age stat
     */
    public Stat parseAgeStat(SearchResult searchResult) {

        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation agecount = aggregations.getTermsAggregation("agecount");

        List<TermsAggregation.Entry> buckets = agecount.getBuckets();

        int lt20count = 0;
        int gt30count = 0;
        int from20to30count = 0;

        for (TermsAggregation.Entry bucket : buckets) {

            if (Integer.parseInt(bucket.getKey()) < 20){

                lt20count += bucket.getCount();

            }else if (Integer.parseInt(bucket.getKey()) >= 30){

                gt30count += bucket.getCount();
            }else{

                from20to30count += bucket.getCount();
            }

        }

        double sumCount = lt20count + gt30count + from20to30count;

        DecimalFormat decimalFormat = new DecimalFormat("###.00");

        String lt20Percent = decimalFormat.format(lt20count / sumCount * 100);
        String gt30Percent = decimalFormat.format(gt30count / sumCount * 100);

        String from20to30Percent = decimalFormat.format(100 - Double.parseDouble(lt20Percent) - Double.parseDouble(gt30Percent));

        List<Option> options=new ArrayList<>();

        options.add(new Option("20--30",from20to30Percent));
        options.add(new Option("30--and above",gt30Percent));
        options.add(new Option("0--20",lt20Percent));

        Stat stat = new Stat("age ratios", options);

        return stat;


    }
    /*
     *   to get the gender stat
     */
    public Stat parseGenderStat(SearchResult searchResult) {

        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation agecount = aggregations.getTermsAggregation("gendercount");

        List<TermsAggregation.Entry> buckets = agecount.getBuckets();

        long maleCount = 0;
        long femaleCount = 0;


        for (TermsAggregation.Entry bucket : buckets) {

            if (bucket.getKey().equals("M")) {

                maleCount = bucket.getCount();

            } else {

                femaleCount = bucket.getCount();
            }

        }


        double sumCount = maleCount + femaleCount;

        DecimalFormat decimalFormat = new DecimalFormat("###.00");

        String malePercent = decimalFormat.format(maleCount / sumCount * 100);


        String femalePercent = decimalFormat.format(100 - Double.parseDouble(malePercent));

        List<Option> options = new ArrayList<>();

        options.add(new Option("male", malePercent));
        options.add(new Option("female", femalePercent));


        Stat stat = new Stat("gender ratios", options);

        return stat;
    }
}
