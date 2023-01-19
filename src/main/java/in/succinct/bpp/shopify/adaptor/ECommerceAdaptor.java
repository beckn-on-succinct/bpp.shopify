package in.succinct.bpp.shopify.adaptor;

import com.venky.core.string.StringUtil;
import com.venky.swf.db.Database;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Quote;
import in.succinct.beckn.Request;
import in.succinct.beckn.Tracking;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.registry.BecknRegistry;
import in.succinct.bpp.search.adaptor.SearchAdaptor;
import in.succinct.bpp.shopify.db.model.BecknOrderMeta;
import in.succinct.bpp.shopify.helpers.ECommerceHelper;
import in.succinct.bpp.shopify.helpers.model.DraftOrder;
import in.succinct.bpp.shopify.helpers.model.Order.Fulfillments;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Map;

public class ECommerceAdaptor extends CommerceAdaptor {
    final SearchAdaptor searchAdaptor;
    final Map<String,String> configuration ;
    final ECommerceHelper helper ;
    public ECommerceAdaptor(Map<String,String> configuration, Subscriber subscriber, BecknRegistry registry){
        super(subscriber,registry);
        this.searchAdaptor = new SearchAdaptor(this);
        this.configuration = configuration;
        this.helper = new ECommerceHelper(this);
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    @Override
    public void search(Request request,Request reply){
        this.searchAdaptor.search(request,reply);
    }

    /* Don't remove Used from AppInstaller */
    /* Search is fulfilled from the plugin */
    public void _search(Request request, Request reply) {
        /* Code here logic to seach your ecommerce application like woocommerce,shopify, ...*/
        /* Return reply compatible with on_search json */
        Message message = helper.createMessage(reply);
        Catalog catalog = helper.createCatalog(message);

    }

    @Override
    public void select(Request request, Request response) {
        /* Take select json and fill response with on_select message */
        Message message = helper.createMessage(response);
        Order outOrder = helper.createOrder(message);
        Quote quote = helper.createQuote(outOrder,request.getMessage().getOrder());

    }

    @Override
    public void init(Request request, Request reply) {
        Order order = request.getMessage().getOrder();
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        /* Take init message and fill response with on_init message */
        Message message = helper.createMessage(reply);

        DraftOrder draftOrder = helper.saveDraftOrder(request);
        Order outOrder = helper.getBecknOrder(draftOrder);

        message.setOrder(outOrder);

    }

    @Override
    public void confirm(Request request, Request reply) {
        Order order = request.getMessage().getOrder();
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        /* Take confirm message and fill response with on_confirm message */
        Message message = helper.createMessage(reply);

        in.succinct.bpp.shopify.helpers.model.Order confirmedOrder = helper.confirmDraftOrder(request);
        Order outOrder = helper.getBecknOrder(confirmedOrder);
        message.setOrder(outOrder);


    }

    @Override
    public void track(Request request, Request reply) {
        /* Take track message and fill response with on_track message */
        Order order = request.getMessage().getOrder();
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        JSONObject params = new JSONObject();
        params.put("fields","tracking_urls");

        JSONObject fulfillmentJson = helper.get(String.format("/orders/%s/fulfillments.json",meta.getECommerceOrderId()),params);
        Fulfillments fulfillments = new Fulfillments((JSONArray) fulfillmentJson.get("fulfillments"));
        String url = fulfillments.get(0).getTrackingUrls().get(0);

        Message message = helper.createMessage(reply);
        message.setTracking(new Tracking());
        message.getTracking().setUrl(url);





    }

    @Override
    public void cancel(Request request, Request reply) {
        Order order = request.getMessage().getOrder();
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        /* Take cancel message and fill response with on_cancel message */
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        JSONObject params = new JSONObject();
        params.put("reason","customer");
        JSONObject response = helper.post(String.format("/orders/%s/cancel.json",meta.getECommerceOrderId()),params);
        in.succinct.bpp.shopify.helpers.model.Order eCommerceOrder = new in.succinct.bpp.shopify.helpers.model.Order((JSONObject) response.get("order"));

        Message message = helper.createMessage(reply);
        message.setOrder(helper.getBecknOrder(eCommerceOrder));



    }

    @Override
    public void update(Request request, Request reply) {
        throw new RuntimeException("Orders cannot be updated. Please cancel and rebook your orders!");
    }

    @Override
    public void status(Request request, Request reply) {
        Order order = request.getMessage().getOrder();
        if (order == null){
            order = new Order();
            order.setId(request.getMessage().get("order_id"));
            request.getMessage().setOrder(order);
        }
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        /* Take status message and fill response with on_status message */
        JSONObject response = helper.get(String.format("/orders/%s.json",meta.getECommerceOrderId()),new JSONObject());
        in.succinct.bpp.shopify.helpers.model.Order eCommerceOrder = new in.succinct.bpp.shopify.helpers.model.Order((JSONObject) response.get("order"));

        Order becknOrder= helper.getBecknOrder(eCommerceOrder);
        Message message = helper.createMessage(reply);
        message.setOrder(becknOrder);
    }

    @Override
    public void rating(Request request, Request reply) {

    }


    @Override
    public void support(Request request, Request reply) {
        Message message = helper.createMessage(reply);
        message.setEmail(helper.getStore().getSupportEmail());

    }

    @Override
    public void get_cancellation_reasons(Request request, Request reply) {

    }

    @Override
    public void get_return_reasons(Request request, Request reply) {

    }

    @Override
    public void get_rating_categories(Request request, Request reply) {

    }

    @Override
    public void get_feedback_categories(Request request, Request reply) {

    }

    @Override
    public void get_feedback_form(Request request, Request reply) {

    }


}
