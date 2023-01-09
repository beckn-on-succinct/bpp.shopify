package in.succinct.bpp.shopify.helpers.model;

import in.succinct.beckn.BecknObjectsWithId;
import in.succinct.bpp.shopify.helpers.model.ProductImages.ProductImage;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ProductImages extends BecknObjectsWithId<ProductImage> {
    public ProductImages() {
    }

    public ProductImages(JSONArray array) {
        super(array);
    }

    public static class ProductImage extends ShopifyObjectWithId{

        public ProductImage() {
        }

        public ProductImage(JSONObject object) {
            super(object);
        }

        public int getWidth(){
            return getInteger("width");
        }

        public int getHeight(){
            return getInteger("height");
        }

        public String getSrc(){
            return get("src");
        }

        public ShopifyIds getVariantIds(){
            return get(ShopifyIds.class, "variant_ids");
        }




    }

}
