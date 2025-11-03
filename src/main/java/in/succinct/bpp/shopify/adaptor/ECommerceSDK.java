package in.succinct.bpp.shopify.adaptor;

import com.venky.cache.Cache;
import com.venky.core.collections.IgnoreCaseMap;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.integration.api.Call;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.integration.api.InputFormat;
import in.succinct.beckn.Order;
import in.succinct.bpp.core.adaptor.TimeSensitiveCache;
import in.succinct.bpp.shopify.model.Products;
import in.succinct.bpp.shopify.model.Products.InventoryItem;
import in.succinct.bpp.shopify.model.Products.InventoryItems;
import in.succinct.bpp.shopify.model.Products.InventoryLevel;
import in.succinct.bpp.shopify.model.Products.InventoryLevels;
import in.succinct.bpp.shopify.model.Products.Product;
import in.succinct.bpp.shopify.model.Products.ProductVariant;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import in.succinct.bpp.shopify.model.Store;
import in.succinct.json.JSONObjectWrapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ECommerceSDK {
    final Map<String,String> creds ;
    public ECommerceSDK(Map<String,String> creds){
        this.creds = creds;
    }
    
    private final TypeConverter<Double> doubleTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter();
    private final TypeConverter<Boolean> booleanTypeConverter  = Database.getJdbcTypeHelper("").getTypeRef(boolean.class).getTypeConverter();

    public <T extends JSONAware> T post(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> T putViaPost(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<>(){{
            put("X-HTTP-Method-Override","PUT");
        }});
    }
    public <T extends JSONAware> T putViaGet(String relativeUrl, JSONObject parameter ){
        return get(relativeUrl,parameter,new IgnoreCaseMap<>(){{
            put("X-HTTP-Method-Override","PUT");
        }});
    }
    public <T extends JSONAware> T delete(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<>(){{
            put("X-HTTP-Method-Override","DELETE");
        }});
    }
    public <T extends JSONAware> T post(String relativeUrl, JSONObject parameter , Map<String,String> addnlHeaders){
        Call<JSONObject> call =  new Call<JSONObject>().url(getAdminApiUrl(),relativeUrl).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).headers(addnlHeaders)
                .inputFormat(InputFormat.JSON).input(parameter).method(HttpMethod.POST);
        T object = call.getResponseAsJson();
        Bucket tries = new Bucket(3);
        while (call.getStatus() > 200 && call.getStatus() < 299 && tries.intValue() > 0){
            List<String> locations = call.getResponseHeaders().get("location");
            if (locations.isEmpty()){
                break;
            }
            String location = locations.get(0);
            List<String> after = call.getResponseHeaders().get("retry-after");
            if (after == null || after.isEmpty()){
                break;
            }
            long millis = Long.parseLong(after.get(0)) * 1000  + 500 ;
            try {
                Thread.currentThread().wait(millis);
            }catch (Exception ex){
                //
            }
            call = new Call<JSONObject>().url(String.format("%s.json" ,location)).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).
                    inputFormat(InputFormat.FORM_FIELDS).input(new JSONObject()).method(HttpMethod.GET);
            object  = call.getResponseAsJson();
            tries.decrement();
        }
        if (call.hasErrors()){
            object =  JSONObjectWrapper.parse(new InputStreamReader(call.getErrorStream()));
        }
        return object;
    }
    public <T extends JSONAware> T graphql(String payload){
        Call<InputStream> call = new Call<InputStream>().url(getAdminApiUrl(),"graphql.json").header("content-type", "application/graphql")
                .header("X-Shopify-Access-Token",getAccessToken())
                .inputFormat(InputFormat.INPUT_STREAM)
                .input(new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)))
                .method(HttpMethod.POST);

        T o = call.getResponseAsJson();

        if (o == null && call.hasErrors()){
            o = JSONObjectWrapper.parse(new InputStreamReader(call.getErrorStream()));
        }
        return o;
    }
    public <T extends JSONAware> T get(String relativeUrl, JSONObject parameter){
        return get(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> T get(String relativeUrl, JSONObject parameter, Map<String,String> addnlHeaders){
        Call<JSONObject> call = new Call<JSONObject>().url(getAdminApiUrl(),relativeUrl).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).
                headers(addnlHeaders)
                .input(parameter)
                .method(HttpMethod.GET);
        T o = call.getResponseAsJson();
        if (o == null && call.hasErrors()){
            o = JSONObjectWrapper.parse(new InputStreamReader(call.getErrorStream()));
        }
        return o;
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
    
    public void delete(ShopifyOrder shopifyOrder) {
        if (!ObjectUtil.isVoid(shopifyOrder.getId())) {
            delete(String.format("/orders/%s.json", shopifyOrder.getId()), new JSONObject());
            shopifyOrder.rm("id");
        }
    }
    
    
    public static class Page<T extends JSONAware> {
        T data;
        String next = null;
        String previous = null;
    }
    
    private String getAccessToken(){
        return creds.get("X-Shopify-Access-Token");
    }
    
    private String getStoreUrl(){
        return creds.get("X-Store-Url");
    }
    
    private String getAdminApiUrl(){
        Date now = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(now);
        int month = calendar.get(Calendar.MONTH) ;
        int m = (month/3) * 3 + 1 ;
        int y = calendar.get(Calendar.YEAR);
        
        
        return String.format("%s/admin/api/%d-%02d", getStoreUrl(), y,m);
    }
    
    /* Shopify Store data utilities
     *
     *
     */
    
    public Store getStore() {
        return cache.get(Store.class, () -> {
            JSONObject storeJson = get("/shop.json", new JSONObject());
            return new Store((JSONObject) storeJson.get("shop"));
        });
    }
    public InventoryLevels getInventoryLevels() {
        return cache.get(InventoryLevel.class,()->{
            JSONObject input = new JSONObject();
            input.put("location_ids", String.valueOf(getStore().getPrimaryLocationId()));
            input.put("limit", 250);
            
            InventoryLevels levels = new InventoryLevels();
            Page<JSONObject> pageInventoryLevels = new Page<>();
            pageInventoryLevels.next = "/inventory_levels.json";
            while (pageInventoryLevels.next != null) {
                pageInventoryLevels = getPage(pageInventoryLevels.next, input);
                input.remove("location_ids");
                if (pageInventoryLevels.data != null) {
                    JSONArray inventoryLevels = (JSONArray) pageInventoryLevels.data.get("inventory_levels");
                    inventoryLevels.forEach(o -> levels.add(new InventoryLevel((JSONObject) o)));
                } else {
                    break;
                }
            }
            return levels;
        });
    }
    public static class InventoryLocations extends Cache<Long,List<Long>> {
        
        @Override
        protected List<Long> getValue(Long inventoryItemId) {
            return new ArrayList<>(); //Locationids
        }
    }
    public InventoryLocations getInventoryLocations(){
        return cache.get(InventoryLocations.class,()->{
            InventoryLocations inventoryLocations = new InventoryLocations();

            InventoryLevels levels = getInventoryLevels();
            levels.forEach(level -> inventoryLocations.get(level.getInventoryItemId()).add(level.getLocationId()));
            
            Products products = getProducts();
            products.forEach(product -> {
                for (ProductVariant productVariant : product.getProductVariants()) {
                    List<Long> shopifyLocationIds = inventoryLocations.get(productVariant.getInventoryItemId()); // Just load keys
                    if (shopifyLocationIds.isEmpty()){
                        shopifyLocationIds.add(getStore().getPrimaryLocationId());
                    }
                }
            });
            return inventoryLocations;
        });
    }
    
    public InventoryItems getInventoryItems() {
        return cache.get(InventoryItems.class,()->{
            InventoryLocations inventoryLocations = getInventoryLocations();
            if (inventoryLocations.isEmpty()) {
                return new InventoryItems();
            }
            
            StringBuilder inventoryItemIds = new StringBuilder();
            for (Iterator<Long> i = inventoryLocations.keySet().iterator(); i.hasNext(); ) {
                inventoryItemIds.append(i.next());
                if (i.hasNext()) {
                    inventoryItemIds.append(",");
                }
            }
            JSONObject input = new JSONObject();
            input.put("ids", inventoryItemIds.toString());
            input.put("limit", 250);
            
            InventoryItems inventoryItems = new InventoryItems();
            Page<JSONObject> pageInventoryItems = new Page<>();
            pageInventoryItems.next = "/inventory_items.json";
            while (pageInventoryItems.next != null) {
                pageInventoryItems = getPage(pageInventoryItems.next, input);
                input.remove("ids");
                if (pageInventoryItems.data != null) {
                    JSONArray inventoryItemsArray = (JSONArray) pageInventoryItems.data.get("inventory_items");
                    inventoryItemsArray.forEach(o -> inventoryItems.add(new InventoryItem((JSONObject) o)));
                } else {
                    break;
                }
            }
            return inventoryItems;
        });
    }
    public static class PublishedProducts extends HashSet<String>{}
    
    private PublishedProducts getPublishedProducts() {
        return cache.get(PublishedProducts.class,()->{
            
            JSONObject input = new JSONObject();
            input.put("limit", 1000);
            PublishedProducts ids = new PublishedProducts();
            
            Page<JSONObject> productsPage = new Page<>();
            productsPage.next = "/product_listings/product_ids.json";
            while (productsPage.next != null) {
                productsPage = getPage(productsPage.next, input);
                if (productsPage.data == null) {
                    break;
                }
                
                JSONArray productIds = ((JSONArray) productsPage.data.get("product_ids"));
                for (Object id : productIds) {
                    ids.add(id.toString());
                }
            }
            return ids;
        });
    }
    public Products getProducts() {
        PublishedProducts specificProductIds = getPublishedProducts();
        
        StringBuilder idsQuery = new StringBuilder();
        for (String id : specificProductIds) {
            if (!idsQuery.isEmpty()) {
                idsQuery.append(",");
            }
            idsQuery.append(id);
        }
        JSONObject input = new JSONObject();
        input.put("limit", 250);
        input.put("ids", idsQuery);
        
        Products products = new Products();
        
        Page<JSONObject> productsPage = new Page<>();
        productsPage.next = "/products.json";
        while (productsPage.next != null) {
            productsPage = getPage(productsPage.next, input);
            input.remove("ids");
            if (productsPage.data == null) {
                break;
            }
            Products productsInPage = new Products((JSONArray) productsPage.data.get("products"));
            for (Product product : productsInPage) {
                if (!product.isActive()) {
                    continue;
                }
                if (product.getProductVariants() == null) {
                    continue;
                }
                products.add(product);
            }
        }
        return products;
    }
    public ShopifyOrder findShopifyOrder(String shopifyOrderId){
        if (shopifyOrderId.startsWith("gid:")) {
            shopifyOrderId = shopifyOrderId.substring(shopifyOrderId.lastIndexOf("/") + 1);
        }
        JSONObject response = get(String.format("/orders/%s.json", shopifyOrderId), new JSONObject());
        return new ShopifyOrder((JSONObject) response.get("order"));
    }
    
    private final TimeSensitiveCache cache = new TimeSensitiveCache(Duration.ofDays(1L));
    

}
