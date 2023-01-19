package in.succinct.bpp.shopify.helpers.model;

import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import in.succinct.beckn.BecknObject;
import in.succinct.beckn.BecknObjectsWithId;

import in.succinct.bpp.shopify.helpers.model.Products.Product;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.Objects;

public class Products extends BecknObjectsWithId<Product> {

    public Products() {
    }

    public Products(JSONArray array) {
        super(array);
    }

    public static class Product extends ShopifyObjectWithId{

        public Product() {
        }

        public Product(JSONObject object) {
            super(object);
        }

        public ProductImages getImages(){
            return get(ProductImages.class, "images");
        }

        public Options getOptions(){
            return get(Options.class, "options");
        }

        public String getProductType(){
            return get("product_type");
        }

        public Date getPublishedAt(){
            return getTimestamp("published_at");
        }

        public String getPublishedScope(){
            return get("published_scope");
        }
        public String getStatus(){
            return get("status");
        }

        public boolean isActive(){
            return Objects.equals(getStatus(),"active");
        }

        public String getTags(){
            return get("tags");
        }

        public String getTitle(){
            return get("title");
        }

        public ProductVariants getProductVariants(){
            return get(ProductVariants.class, "product_variants");
        }




    }

    public static class ProductVariants extends BecknObjectsWithId<ProductVariant>{

        public ProductVariants() {
        }

        public ProductVariants(JSONArray array) {
            super(array);
        }
    }

    public static class ProductVariant extends ShopifyObjectWithId {

        public ProductVariant() {
        }

        public ProductVariant(JSONObject object) {
            super(object);
        }
        
        public String getBarCode(){
            return get("bar_code");
        }
        
        public int getGrams(){
            return getInteger("grams");
        }
        
        public long getImageId(){
            return getLong("image_id");
        }
        
        public long getInventoryItemId(){
            return getLong("inventory_item_id");
        }
        public double getMrp(){
            return getDouble("compare_at_price");
        }


        
        public double getInventoryQuantity(){
            return getDouble("inventory_quantity");
        }

        public ProductOption getProductOption(){
            return get(ProductOption.class, "option");
        }

        public double getPrice(){
            return getDouble("price");
        }

        public boolean getTaxable(){
            return getBoolean("taxable");
        }

        public String getTitle(){
            return get("title");
        }

        public String getProductId(){
            return StringUtil.valueOf(get("product_id"));
        }


    }
    public static class ProductOption extends BecknObject {
        public String getOption1(){
            return get("option1");
        }

        public String getOption2(){
            return get("option2");
        }

        public String getOption3(){
            return get("option3");
        }
    }
    public static class InventoryItems extends BecknObjectsWithId<InventoryItem> {

        public InventoryItems() {
        }

        public InventoryItems(JSONArray array) {
            super(array);
        }
    }

    public static class InventoryItem extends ShopifyObjectWithId{

        public InventoryItem() {
        }

        public InventoryItem(JSONObject object) {
            super(object);
        }

        public String getCountryCodeOfOrigin(){
            return get("country_code_of_origin");
        }

        public String getHarmonizedSystemCode(){
            return get("harmonized_system_code");
        }

        public String getProvinceCodeOfOrigin(){
            return get("province_code_of_origin");
        }

        public String getSku(){
            return get("sku");
        }

        public boolean getTracked(){
            return getBoolean("tracked");
        }

        public boolean getRequiresShipping(){
            return getBoolean("requires_shipping");
        }
        
        
    }
    public static class InventoryLevels extends BecknObjectsWithId<InventoryLevel> {

        public InventoryLevels() {
        }

        public InventoryLevels(JSONArray array) {
            super(array);
        }
    }
    public static class InventoryLevel extends ShopifyObjectWithId {
        public InventoryLevel(){
            super();
        }
        public InventoryLevel(JSONObject level){
            super(level);
        }

        public long getInventoryItemId(){
            return getLong("inventory_item_id");
        }
        public long getLocationId(){
            return getLong("location_id");
        }

        public boolean isAvailable(){
            return get("available") == null || (Double)get("available") > 0.0D;
        }


    }
}
