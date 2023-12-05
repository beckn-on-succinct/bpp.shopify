package in.succinct.bpp.shopify.extensions;

import com.venky.core.date.DateUtils;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Registry;
import com.venky.swf.path.Path;
import in.succinct.beckn.Context;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.Return;
import in.succinct.beckn.Order.Return.ReturnStatus;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Date;
import java.util.UUID;
import java.util.logging.Logger;

public class ReturnWebhook extends ShopifyWebhook {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.return_hook",new ReturnWebhook());
    }

    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor,Path path, String payload) {
        String event = path.parameter();
        /*if (ObjectUtil.equals(path.getHeaders().get("X-Shopify-Topic"),"returns/request")) {
            event = "on_update"; // If On_update is delayed.!!
        }*/

        JSONObject eReturn = (JSONObject) JSONValue.parse(payload);

        String returnId = StringUtil.valueOf(eReturn.get("admin_graphql_api_id"));
        JSONObject jsonOrder = ((JSONObject)eReturn.get("order"));
        if (jsonOrder == null){
            JSONObject eReturn1 = eCommerceAdaptor.getReturn(returnId);
            eReturn.putAll(eReturn1);
            jsonOrder= ((JSONObject)eReturn.get("order"));
        }
        String orderId = String.valueOf(jsonOrder.get("id"));
        ShopifyOrder shopifyOrder = eCommerceAdaptor.getShopifyOrder(orderId);


        LocalOrderSynchronizer localOrderSynchronizer = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(eCommerceAdaptor.getSubscriber());

        Date now = new Date();
        Order lastKnownOrder = localOrderSynchronizer.getLastKnownOrder(eCommerceAdaptor.getBecknTransactionId(shopifyOrder),true);
        lastKnownOrder.setUpdatedAt(now);
        Return returnReference = lastKnownOrder.getReturns().get(returnId);
        returnReference.setReturnStatus(ReturnStatus.convertor.valueOf(((String)eReturn.get("status")).toUpperCase() ));
        localOrderSynchronizer.sync(eCommerceAdaptor.getBecknTransactionId(shopifyOrder),lastKnownOrder);

        final Request request = new Request();
        request.setMessage(new Message());
        request.setContext(new Context());

        Context context = request.getContext();
        context.setBppId(eCommerceAdaptor.getSubscriber().getSubscriberId());
        context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
        context.setTimestamp(now);
        context.setAction(event);
        context.setDomain(eCommerceAdaptor.getSubscriber().getDomain());
        shopifyOrder.getNoteAttributes().forEach(na->{
            if (na.getName().startsWith("context.")){
                String key = na.getName().substring("context.".length());
                context.set(key,na.getValue());
            }
        });


        //Fill any other attributes needed.
        //Send unsolicited on_status.
        context.setMessageId(returnReference.getReturnMessageId() == null ? UUID.randomUUID().toString() : returnReference.getReturnMessageId());
        Order finalOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder);
        request.getMessage().setOrder(finalOrder); //updated order.


        networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,request);
    }
}
