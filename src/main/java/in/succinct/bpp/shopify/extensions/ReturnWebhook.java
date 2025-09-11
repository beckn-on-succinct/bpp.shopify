package in.succinct.bpp.shopify.extensions;

import com.venky.core.string.StringUtil;
import com.venky.extension.Registry;
import com.venky.swf.path.Path;
import in.succinct.beckn.Context;
import in.succinct.beckn.Invoice;
import in.succinct.beckn.Invoice.Dispute;
import in.succinct.beckn.Invoice.Dispute.Disputes;
import in.succinct.beckn.Invoice.Dispute.Status;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Request;
import in.succinct.beckn.ReturnReasons.ReturnRejectReason;
import in.succinct.bpp.core.adaptor.api.NetworkApiAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor.ReturnDecline;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import in.succinct.onet.core.adaptor.NetworkAdaptor;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Date;
import java.util.UUID;

public class ReturnWebhook extends ShopifyWebhook {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.return_hook",new ReturnWebhook());
    }

    private static final String RETURN_STATUS_CANCELLED = "CANCELLED";
    private static final String RETURN_STATUS_CLOSED = "CLOSED";
    private static final String RETURN_STATUS_DECLINED = "DECLINED";
    private static final String RETURN_STATUS_OPEN = "OPEN";
    private static final String RETURN_STATUS_REQUESTED = "REQUESTED";
    Status getDisputeStatus(JSONObject eReturn){
        String returnStatus = ((String)eReturn.get("status")).toUpperCase();
        Status status = null;
        switch (returnStatus){
            case RETURN_STATUS_OPEN,RETURN_STATUS_REQUESTED -> {
                status =  Status.Open;
            }
            case RETURN_STATUS_CANCELLED , RETURN_STATUS_DECLINED -> {
                status = Status.Closed;
            }
            case RETURN_STATUS_CLOSED -> {
                // Can be partitalAuthorized
                status = Status.AmountAuthorized;
            }
        }
        return status;
    }
    
    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor, Path path, String payload) {
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
        
        Invoice invoice = lastKnownOrder.getInvoice(lastKnownOrder.getFulfillments().get(0).getId());
        Disputes disputes  = invoice.getDisputes(true);
        
        
        Dispute returnReference = disputes.get(returnId); // Created in update api
        
        returnReference.setStatus(getDisputeStatus(eReturn));
        
        if (returnReference.getStatus() == Status.Closed){
            ReturnDecline decline = new ReturnDecline((JSONObject) eReturn.get("decline"));
            switch (decline.getReturnDeclineReason()){
                case FINAL_SALE:
                    returnReference.setRejectReason(ReturnRejectReason.FINAL_SALE.toString());
                    break;
                case RETURN_PERIOD_ENDED:
                    returnReference.setRejectReason(ReturnRejectReason.RETURN_PERIOD_ENDED.toString());
                    break;
                case OTHER:
                    if (decline.getNote() != null) {
                        String note = decline.getNote().toUpperCase();
                        if (note.contains("DAMAGE")){
                            if (note.contains("PACKAGE")) {
                                returnReference.setRejectReason(ReturnRejectReason.ITEM_PACKAGING_DAMAGED.toString());
                            }else {
                                returnReference.setRejectReason(ReturnRejectReason.ITEM_DAMAGED.toString());
                            }
                        }else if (note.contains("USED")){
                            returnReference.setRejectReason(ReturnRejectReason.ITEM_USED.toString());
                        }else if (note.matches("IN[ \t]*COMPLETE")){
                            returnReference.setRejectReason(ReturnRejectReason.ITEM_INCOMPLETE.toString());
                        }
                    }
                    break;
                default:
                    break;
            }
            if (returnReference.getRejectReason() == null){
                returnReference.setRejectReason(ReturnRejectReason.FINAL_SALE.toString());
            }
        }
        localOrderSynchronizer.sync(eCommerceAdaptor.getBecknTransactionId(shopifyOrder),lastKnownOrder);/// getBecknOrder uses lastKnownObject to update latestOrder object

        final Request request = new Request();
        request.setMessage(new Message());
        request.setContext(shopifyOrder.getContext());

        Context context = request.getContext();
        context.setBppId(eCommerceAdaptor.getSubscriber().getSubscriberId());
        context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
        context.setTimestamp(now);
        context.setAction(event);
        //Domain is stored in shopify order no need to set default one here



        //Fill any other attributes needed.
        //Send unsolicited on_status.
        context.setMessageId(returnReference.getDisputeMessageId() == null ? UUID.randomUUID().toString() : returnReference.getDisputeMessageId());

        Order finalOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder); // Refreshed Again. after updates to laskKnownOrder
        request.getMessage().setOrder(finalOrder); //updated order.


        ((NetworkApiAdaptor)networkAdaptor.getApiAdaptor()).callback(eCommerceAdaptor,request);
    }
}
