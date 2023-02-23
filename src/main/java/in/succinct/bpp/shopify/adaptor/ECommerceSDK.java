package in.succinct.bpp.shopify.adaptor;

import com.venky.core.collections.IgnoreCaseMap;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.db.model.application.ApplicationUtil;
import com.venky.swf.integration.api.Call;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.integration.api.InputFormat;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ECommerceSDK {
    final ECommerceAdaptor adaptor;
    final Application application ;
    public ECommerceSDK(ECommerceAdaptor adaptor){
        this.adaptor = adaptor;
        this.application = adaptor == null ? null : ApplicationUtil.find(adaptor.getSubscriber().getAppId());
    }
    public ECommerceSDK(){
        this(null);
    }
    public String getConfigPrefix(){
        return "in.succinct.bpp.shopify";
    }


    public String getStoreUrl(){
        return adaptor.getConfiguration().get(String.format("%s.storeUrl",getConfigPrefix()));
    }
    public String getAdminApiUrl(){
        Date now = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(now);
        int month = calendar.get(Calendar.MONTH) ;
        int m = (month/3) * 3 + 1 ;
        int y = calendar.get(Calendar.YEAR);


        return String.format("%s/admin/api/%d-%02d", getStoreUrl(), y,m);
    }
    public String getAccessToken(){
        return adaptor.getConfiguration().get(String.format("%s.accessToken",getConfigPrefix()));
    }


    private TypeConverter<Double> doubleTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter();
    private TypeConverter<Boolean> booleanTypeConverter  = Database.getJdbcTypeHelper("").getTypeRef(boolean.class).getTypeConverter();






    public <T extends JSONAware> T post(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> T putViaPost(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<String>(){{
            put("X-HTTP-Method-Override","PUT");
        }});
    }
    public <T extends JSONAware> T putViaGet(String relativeUrl, JSONObject parameter ){
        return get(relativeUrl,parameter,new IgnoreCaseMap<String>(){{
            put("X-HTTP-Method-Override","PUT");
        }});
    }
    public <T extends JSONAware> T delete(String relativeUrl, JSONObject parameter ){
        return get(relativeUrl,parameter,new IgnoreCaseMap<>(){{
            put("X-HTTP-Method-Override","DELETE");
        }});
    }
    public <T extends JSONAware> T post(String relativeUrl, JSONObject parameter , Map<String,String> addnlHeaders){
        return new Call<JSONObject>().url(getAdminApiUrl(),relativeUrl).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).headers(addnlHeaders)
                .inputFormat(InputFormat.JSON).input(parameter).method(HttpMethod.POST).getResponseAsJson();
    }
    public <T extends JSONAware> T get(String relativeUrl, JSONObject parameter){
        return get(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> T get(String relativeUrl, JSONObject parameter, Map<String,String> addnlHeaders){
        return new Call<JSONObject>().url(getAdminApiUrl(),relativeUrl).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).headers(addnlHeaders)
                .input(parameter)
                .method(HttpMethod.GET).getResponseAsJson();
    }

    public <T extends JSONAware> Page<T> getPage(String relativeUrl, JSONObject parameter){
        return getPage(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> Page<T> getPage(String relativeUrl, JSONObject parameter, Map<String,String> addnlHeaders){
        Call<JSONObject> call = new Call<JSONObject>().url(getAdminApiUrl(),relativeUrl).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).headers(addnlHeaders)
                .input(parameter)
                .method(HttpMethod.GET);

        Page<T> page = new Page<>();
        page.data = call.getResponseAsJson();
        List<String> links = call.getResponseHeaders().get("Link");
        if (links != null && !links.isEmpty()){
            String link = links.get(0);
            Matcher matcher = Pattern.compile("(<)(https://[^\\?]*\\?page_info=[^&]*&limit=[0-9]*)(>; rel=)(previous|next)([ ,])*").matcher(link);
            matcher.results().forEach(mr->{
                if (mr.group(4).equals("next")) {
                    page.next = mr.group(2);
                }else {
                    page.previous = mr.group(2);
                }
            });
        }


        return page;
    }



    public static class Page<T extends JSONAware> {
        T data;
        String next = null;
        String previous = null;
    }

}
