package in.succinct.bpp.shopify.model;

import in.succinct.beckn.BecknObjectsWithId;
import in.succinct.beckn.BecknStrings;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Order extends DraftOrder{
    public Order(){
        super();
    }
    public Order(JSONObject eCommerceOrder) {
        super(eCommerceOrder);
    }

    public static class Fulfillment extends ShopifyObjectWithId{

        public Fulfillment(JSONObject fulfillment){
            super(fulfillment);
        }
        public BecknStrings getTrackingUrls(){
            return get(BecknStrings.class, "tracking_urls");
        }
        
    }
    public static class Fulfillments extends BecknObjectsWithId<Fulfillment> {
        public Fulfillments(){
            super();
        }
        public Fulfillments(JSONArray array){
            super(array);
        }
    }

}
