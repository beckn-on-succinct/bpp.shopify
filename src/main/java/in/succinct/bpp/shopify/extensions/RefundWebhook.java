package in.succinct.bpp.shopify.extensions;

import com.venky.core.date.DateUtils;
import com.venky.core.security.Crypt;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper;
import com.venky.swf.path.Path;
import freemarker.template.utility.DateUtil;
import in.succinct.beckn.Amount;
import in.succinct.beckn.Context;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.Return;
import in.succinct.beckn.Order.Return.ReturnStatus;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transaction;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transactions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;

public class RefundWebhook extends ShopifyWebhook {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.refund_hook",new RefundWebhook());
    }

    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor,Path path, String payload) {
        String event = path.parameter();


        JSONObject ePayload = (JSONObject) JSONValue.parse(payload);
        String refundId = String.valueOf(ePayload.get("id"));

        String orderId = StringUtil.valueOf(ePayload.get("order_id"));
        JSONObject eReturn = (JSONObject) ePayload.get("return");
        String returnId = StringUtil.valueOf(eReturn.get("admin_graphql_api_id"));


        Transactions transactions = new Transactions((JSONArray) ePayload.get("transactions"));



        ShopifyOrder shopifyOrder = eCommerceAdaptor.getShopifyOrder(orderId);

        LocalOrderSynchronizer localOrderSynchronizer = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(eCommerceAdaptor.getSubscriber());
        Order lastKnownOrder = localOrderSynchronizer.getLastKnownOrder(eCommerceAdaptor.getBecknTransactionId(shopifyOrder),true);

        Date deliveryTs = new Date();
        Date pickupTs = new Date(deliveryTs.getTime()-60*1000L); // 1 minute.! // Fudged!! TODO Get from logistics provider

        Return returnReference = lastKnownOrder.getReturns().get(returnId);
        returnReference.setReturnStatus(ReturnStatus.REFUNDED);
        returnReference.getFulfillment().setFulfillmentStatus(FulfillmentStatus.Order_delivered);

        returnReference.getFulfillment().getStart().getTime().setTimestamp(pickupTs); // We dont know when it was picked and delivered
        returnReference.getFulfillment().getStart().getTime().getRange().setStart(DateUtils.min(returnReference.getFulfillment().getStart().getTime().getRange().getStart(),pickupTs));
        returnReference.getFulfillment().getStart().getTime().getRange().setEnd(DateUtils.max(returnReference.getFulfillment().getStart().getTime().getRange().getEnd(),pickupTs));

        returnReference.getFulfillment().getEnd().getTime().setTimestamp(deliveryTs);// We dont know when it was picked and delivered
        returnReference.getFulfillment().getEnd().getTime().getRange().setStart(DateUtils.min(returnReference.getFulfillment().getEnd().getTime().getRange().getStart(),deliveryTs));
        returnReference.getFulfillment().getEnd().getTime().getRange().setEnd(DateUtils.max(returnReference.getFulfillment().getEnd().getTime().getRange().getEnd(),deliveryTs));


        returnReference.setRefund(new Amount());
        returnReference.setRefundId(refundId);

        Bucket amount = new Bucket();
        for (Transaction transaction : transactions) {
            returnReference.getRefund().setCurrency(transaction.getCurrency());
            amount.increment(transaction.getAmount());
        }
        returnReference.getRefund().setValue(amount.doubleValue());

        localOrderSynchronizer.sync(eCommerceAdaptor.getBecknTransactionId(shopifyOrder),lastKnownOrder);

        Order becknOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder);
        becknOrder.setUpdatedAt(deliveryTs);


        final Request request = new Request();
        request.setMessage(new Message());
        request.setContext(new Context());
        request.getMessage().setOrder(becknOrder);
        Context context = request.getContext();
        context.setBppId(eCommerceAdaptor.getSubscriber().getSubscriberId());
        context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
        context.setTimestamp(deliveryTs);
        context.setAction(event);
        context.setDomain(eCommerceAdaptor.getSubscriber().getDomain());
        shopifyOrder.getNoteAttributes().forEach(na->{
            if (na.getName().startsWith("context.")){
                String key = na.getName().substring("context.".length());
                context.set(key,na.getValue());
            }
        });


        //Fill any other attributes needed.
        //Send unsolicited on_status.'

        context.setMessageId(returnReference.getReturnMessageId() == null ? UUID.randomUUID().toString() : returnReference.getReturnMessageId());
        networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,request);
    }
}
