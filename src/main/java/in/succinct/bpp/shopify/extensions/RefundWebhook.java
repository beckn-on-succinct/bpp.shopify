package in.succinct.bpp.shopify.extensions;

import com.venky.core.date.DateUtils;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.extension.Registry;
import com.venky.swf.path.Path;
import in.succinct.beckn.CancellationReasons.CancellationReasonCode;
import in.succinct.beckn.Context;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.Fulfillment.RetailFulfillmentType;
import in.succinct.beckn.Invoice;
import in.succinct.beckn.Invoice.Dispute;
import in.succinct.beckn.Invoice.Dispute.Credit;
import in.succinct.beckn.Invoice.Dispute.Credit.Credits;
import in.succinct.beckn.Item;
import in.succinct.beckn.Items;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.Status;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.api.NetworkApiAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import in.succinct.bpp.shopify.model.ShopifyOrder.LineItem;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transaction;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transactions;
import in.succinct.onet.core.adaptor.NetworkAdaptor;
import in.succinct.onet.core.api.BecknIdHelper;
import in.succinct.onet.core.api.BecknIdHelper.Entity;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class RefundWebhook extends ShopifyWebhook {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.refund_hook",new RefundWebhook());
    }

    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor, Path path, String payload) {
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
        Set<String> refundIds = new HashSet<>();
        for (Invoice invoice : lastKnownOrder.getInvoices()) {
            for (Dispute dispute : invoice.getDisputes()) {
                for (Credit credit : dispute.getCredits()) {
                    refundIds.add(credit.getId());
                }
            }
        }
        
        if (refundIds.contains(refundId) ){
            // Refund hook called for refund already processed for the order.
                return; // Do nothing
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

            Invoice invoice  = lastKnownOrder.getInvoice(lastKnownOrder.getFulfillment().getId());
            
            Dispute returnReference = invoice.getDisputes().get(returnId);
            
            Fulfillment returnFulfillment = lastKnownOrder.getFulfillments().get(returnReference.getId()); // DisputeId is the fulfillment Id also
            
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
            
            returnReference.setStatus(Dispute.Status.AmountAuthorized);
            if (!liquidated) {
                returnFulfillment.setFulfillmentStatus(FulfillmentStatus.Completed);
            }else {
                returnFulfillment.setFulfillmentStatus(FulfillmentStatus.Cancelled);
            }


            returnFulfillment._getStart().getTime().setTimestamp(pickupTs); // We dont know when it was picked and delivered
            returnFulfillment._getStart().getTime().getRange().setStart(DateUtils.min(returnFulfillment._getStart().getTime().getRange().getStart(),pickupTs));
            returnFulfillment._getStart().getTime().getRange().setEnd(DateUtils.max(returnFulfillment._getStart().getTime().getRange().getEnd(),pickupTs));

            returnFulfillment._getEnd().getTime().setTimestamp(deliveryTs);// We dont know when it was picked and delivered
            returnFulfillment._getEnd().getTime().getRange().setStart(DateUtils.min(returnFulfillment._getEnd().getTime().getRange().getStart(),deliveryTs));
            returnFulfillment._getEnd().getTime().getRange().setEnd(DateUtils.max(returnFulfillment._getEnd().getTime().getRange().getEnd(),deliveryTs));

            Credits credits = returnReference.getCredits(true);
            Credit credit = new Credit();
            credit.setCurrency(lastKnownOrder.getQuote().getPrice().getCurrency());
            credit.setAmount(refundedAmount.doubleValue());
            credit.setDate(new Date());
            credit.setId(refundId);
            credits.add(credit);
            
            lastKnownOrder.getFulfillments().add(returnFulfillment,true);
            context.setTimestamp(deliveryTs);
            context.setMessageId(returnReference.getDisputeMessageId());
            context.setAction("on_update");
        }else {
            context.setMessageId(UUID.randomUUID().toString());
            Invoice invoice = lastKnownOrder.getInvoice(lastKnownOrder.getFulfillment().getId());
            
            Dispute dummy = new Dispute();
            dummy.setStatus(Dispute.Status.AmountAuthorized);
            dummy.setDisputeAmount(0);
            dummy.setAuthorizedAmount(refundedAmount.doubleValue());
            dummy.setReason(CancellationReasonCode.convertor.toString(CancellationReasonCode.ITEM_NOT_AVAILABLE));
            dummy.setId("refund:"+UUID.randomUUID().toString());
            dummy.setItems(new Items());
            Credit refund = new Credit();
            refund.setId(refundId);
            refund.setDate(new Date());
            refund.setAmount(refundedAmount.doubleValue());
            refund.setCurrency(lastKnownOrder.getQuote().getPrice().getCurrency());
            
            
            Fulfillment refundFulfillment = new Fulfillment();
            refundFulfillment.setId(dummy.getId());
            refundFulfillment.setType(RetailFulfillmentType.cancel.toString());
            refundFulfillment.setFulfillmentStatus(FulfillmentStatus.Cancelled);

            
            // Merchant Cancel
            JSONArray refundLineItems = (JSONArray) ePayload.get("refund_line_items");
            for (Object refundLineItem : refundLineItems) {
                LineItem lineItem = new LineItem((JSONObject) ((JSONObject) refundLineItem).get("line_item"));
                String itemId = BecknIdHelper.getBecknId(String.valueOf(lineItem.getVariantId()), eCommerceAdaptor.getSubscriber(), Entity.item);

                Item item = new Item(lastKnownOrder.getItems().get(itemId).toString());
                Quantity q = new Quantity();
                int qty = Integer.parseInt(StringUtil.valueOf(((JSONObject) refundLineItem).get("quantity")));
                q.setCount(qty);
                item.setQuantity(q);
                item.setFulfillmentId(refundFulfillment.getId());
                item.setPayload(item.getInner().toString());
                dummy.getItems().add(item);
            }
            
            dummy.getCredits(true).add(refund,true);
            invoice.getDisputes().add(dummy);
            
            lastKnownOrder.getFulfillments().add(refundFulfillment,true);

            if (shopifyOrder.getStatus() == Status.Cancelled){
                context.setAction("on_cancel");
            }else {
                context.setAction("on_update");
            }
        }


        localOrderSynchronizer.sync(eCommerceAdaptor.getBecknTransactionId(shopifyOrder),lastKnownOrder);
        Order becknOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder);
        becknOrder.setUpdatedAt(context.getTimestamp());


        request.getMessage().setOrder(becknOrder);



        //Fill any other attributes needed.
        //Send unsolicited on_status.'
        ((NetworkApiAdaptor)networkAdaptor.getApiAdaptor()).callback(eCommerceAdaptor,request);
    }
}
