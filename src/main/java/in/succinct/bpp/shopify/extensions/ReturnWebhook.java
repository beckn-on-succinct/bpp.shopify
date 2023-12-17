package in.succinct.bpp.shopify.extensions;

import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Registry;
import com.venky.swf.path.Path;
import in.succinct.beckn.Context;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.Return;
import in.succinct.beckn.Order.Return.ReturnStatus;
import in.succinct.beckn.Request;
import in.succinct.beckn.ReturnReasons.ReturnRejectReason;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor.ReturnDecline;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Date;
import java.util.UUID;

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
        Return returnReference = lastKnownOrder.getReturns().get(returnId); // Created in update api
        returnReference.setReturnStatus(ReturnStatus.convertor.valueOf(((String)eReturn.get("status")).toUpperCase() ));
        if (returnReference.getReturnStatus() == ReturnStatus.DECLINED){
            ReturnDecline decline = new ReturnDecline((JSONObject) eReturn.get("decline"));
            switch (decline.getReturnDeclineReason()){
                case FINAL_SALE:
                    returnReference.setReturnRejectReason(ReturnRejectReason.FINAL_SALE);
                    break;
                case RETURN_PERIOD_ENDED:
                    returnReference.setReturnRejectReason(ReturnRejectReason.RETURN_PERIOD_ENDED);
                    break;
                case OTHER:
                    if (decline.getNote() != null) {
                        String note = decline.getNote().toUpperCase();
                        if (note.contains("DAMAGE")){
                            if (note.contains("PACKAGE")) {
                                returnReference.setReturnRejectReason(ReturnRejectReason.ITEM_PACKAGING_DAMAGED);
                            }else {
                                returnReference.setReturnRejectReason(ReturnRejectReason.ITEM_DAMAGED);
                            }
                        }else if (note.contains("USED")){
                            returnReference.setReturnRejectReason(ReturnRejectReason.ITEM_USED);
                        }else if (note.matches("IN[ \t]*COMPLETE")){
                            returnReference.setReturnRejectReason(ReturnRejectReason.ITEM_INCOMPLETE);
                        }
                    }
                    break;
                default:
                    break;
            }
            if (returnReference.getReturnRejectReason() == null){
                returnReference.setReturnRejectReason(ReturnRejectReason.FINAL_SALE);
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
        context.setMessageId(returnReference.getReturnMessageId() == null ? UUID.randomUUID().toString() : returnReference.getReturnMessageId());

        Order finalOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder); // Refreshed Again. after updates to laskKnownOrder
        request.getMessage().setOrder(finalOrder); //updated order.


        networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,request);
    }
}
