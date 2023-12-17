package in.succinct.bpp.shopify.extensions;

import com.venky.core.date.DateUtils;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.extension.Registry;
import com.venky.swf.path.Path;
import in.succinct.beckn.Amount;
import in.succinct.beckn.Context;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.Fulfillment.FulfillmentType;
import in.succinct.beckn.Item;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.NonUniqueItems;
import in.succinct.beckn.Order.Refund;
import in.succinct.beckn.Order.Refunds;
import in.succinct.beckn.Order.Return;
import in.succinct.beckn.Order.Return.ReturnStatus;
import in.succinct.beckn.Order.Status;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper.Entity;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import in.succinct.bpp.shopify.model.ShopifyOrder.LineItem;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transaction;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transactions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

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
        Transactions transactions = new Transactions((JSONArray) ePayload.get("transactions"));
        Bucket refundedAmount = new Bucket();
        for (Transaction transaction : transactions) {
            refundedAmount.increment(transaction.getAmount());
        }
        ShopifyOrder shopifyOrder = eCommerceAdaptor.getShopifyOrder(orderId);

        LocalOrderSynchronizer localOrderSynchronizer = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(eCommerceAdaptor.getSubscriber());
        Order lastKnownOrder = localOrderSynchronizer.getLastKnownOrder(eCommerceAdaptor.getBecknTransactionId(shopifyOrder),true);
        if (lastKnownOrder.getRefunds() == null){
            lastKnownOrder.setRefunds(new Refunds());
        }

        final Request request = new Request();
        request.setMessage(new Message());
        request.setContext(shopifyOrder.getContext());
        Context context = request.getContext();
        context.setBppId(eCommerceAdaptor.getSubscriber().getSubscriberId());
        context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
        context.setAction(event);
        context.setTimestamp(new Date());

        if (eReturn != null){
            //It is a return refund
            boolean liquidated = false;
            String returnId = StringUtil.valueOf(eReturn.get("admin_graphql_api_id"));
            JSONObject eReturn1 = eCommerceAdaptor.getReturn(returnId); //Get other details of the return e.g reverseFulfillmentOrders
            if (eReturn1 != null) {
                eReturn.putAll(eReturn1);
            }

            Date deliveryTs = new Date();
            Date pickupTs = new Date(deliveryTs.getTime()-60*1000L); // 1 minute.! // Fudged!! TODO Get from logistics provider


            Return returnReference = lastKnownOrder.getReturns().get(returnId);
            Fulfillment returnFulfillment = lastKnownOrder.getFulfillments().get(returnReference.getFulfillmentId());
            JSONArray array = (JSONArray) (((JSONObject)eReturn.get("reverseFulfillmentOrders")).get("edges"));

            if ( array != null && ! array.isEmpty()){
                JSONObject node = (JSONObject) (((JSONObject) array.get(0)).get("node"));
                JSONArray deliveries = (JSONArray) (((JSONObject)node.get("reverseDeliveries")).get("edges"));
                liquidated = deliveries == null || deliveries.isEmpty();
                if (!liquidated){
                    JSONObject reverseDelivery = (JSONObject)((JSONObject)deliveries.get(0)).get("node");
                    JSONObject reverseDeliverable = reverseDelivery == null ? null : (JSONObject) reverseDelivery.get("deliverable");
                    JSONObject tracking = reverseDeliverable == null ? null  : (JSONObject) reverseDeliverable.get("tracking");

                    if (tracking != null) {

                        returnFulfillment.setTag("tracking", "gps_enabled", false);
                        returnFulfillment.setTag("tracking", "url_enabled", false);
                        returnFulfillment.setTag("tracking", "url", tracking.get("url"));
                        returnFulfillment.setTag("tracking", "carrier", tracking.get("carrierName"));
                        returnFulfillment.setTag("tracking", "tracking_number", tracking.get("number"));
                    }
                }
            }


            returnReference.setReturnStatus(ReturnStatus.REFUNDED);
            if (!liquidated) {
                returnFulfillment.setFulfillmentStatus(FulfillmentStatus.Return_Delivered);
            }else {
                returnFulfillment.setFulfillmentStatus(FulfillmentStatus.Return_Liquidated);
            }


            returnFulfillment.getStart().getTime().setTimestamp(pickupTs); // We dont know when it was picked and delivered
            returnFulfillment.getStart().getTime().getRange().setStart(DateUtils.min(returnFulfillment.getStart().getTime().getRange().getStart(),pickupTs));
            returnFulfillment.getStart().getTime().getRange().setEnd(DateUtils.max(returnFulfillment.getStart().getTime().getRange().getEnd(),pickupTs));

            returnFulfillment.getEnd().getTime().setTimestamp(deliveryTs);// We dont know when it was picked and delivered
            returnFulfillment.getEnd().getTime().getRange().setStart(DateUtils.min(returnFulfillment.getEnd().getTime().getRange().getStart(),deliveryTs));
            returnFulfillment.getEnd().getTime().getRange().setEnd(DateUtils.max(returnFulfillment.getEnd().getTime().getRange().getEnd(),deliveryTs));

            returnReference.setRefund(new Amount());
            returnReference.setRefundId(refundId);
            returnReference.getRefund().setValue(refundedAmount.doubleValue());
            returnReference.getRefund().setCurrency(lastKnownOrder.getQuote().getPrice().getCurrency());
            Refund refund = new Refund();
            refund.setId(refundId);
            refund.setCreatedAt(new Date());
            refund.setItems(returnReference.getItems());
            refund.setFulfillmentId(returnFulfillment.getId());
            lastKnownOrder.getRefunds().add(refund);
            lastKnownOrder.getFulfillments().add(returnFulfillment,true);
            context.setTimestamp(deliveryTs);
            context.setMessageId(returnReference.getReturnMessageId());
        }else {
            context.setMessageId(UUID.randomUUID().toString());
            Refund refund = new Refund();
            refund.setId(refundId);
            refund.setCreatedAt(new Date());

            refund.setItems(new NonUniqueItems());

            Fulfillment refundFulfillment = null;
            if (shopifyOrder.getStatus() == Status.Cancelled){
                refundFulfillment = lastKnownOrder.getPrimaryFulfillment();
            }else {
                refundFulfillment = new Fulfillment();
                refundFulfillment.setId("refund:"+UUID.randomUUID());
                refundFulfillment.setType(FulfillmentType.cancel);
            }

            refundFulfillment.setFulfillmentStatus(FulfillmentStatus.Cancelled);

            // Merchant Cancel
            JSONArray refundLineItems = (JSONArray) ePayload.get("refund_line_items");
            for (Object refundLineItem : refundLineItems){
                LineItem lineItem = new LineItem((JSONObject)((JSONObject)refundLineItem).get("line_item"));
                String itemId = BecknIdHelper.getBecknId(String.valueOf(lineItem.getVariantId()), eCommerceAdaptor.getSubscriber(), Entity.item);

                Item item = new Item(lastKnownOrder.getItems().get(itemId).toString());
                Quantity q = new Quantity();
                int qty = Integer.parseInt(StringUtil.valueOf(((JSONObject)refundLineItem).get("quantity")));
                q.setCount(qty);
                item.setQuantity(q);
                item.setFulfillmentId(refundFulfillment.getId());
                item.setPayload(item.getInner().toString());
                refund.getItems().add(item);
            }
            refund.setFulfillmentId(refundFulfillment.getId());
            lastKnownOrder.getRefunds().add(refund);
            lastKnownOrder.getFulfillments().add(refundFulfillment,true);
        }


        localOrderSynchronizer.sync(eCommerceAdaptor.getBecknTransactionId(shopifyOrder),lastKnownOrder);
        Order becknOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder);
        becknOrder.setUpdatedAt(context.getTimestamp());


        request.getMessage().setOrder(becknOrder);



        //Fill any other attributes needed.
        //Send unsolicited on_status.'
        networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,request);
    }
}
