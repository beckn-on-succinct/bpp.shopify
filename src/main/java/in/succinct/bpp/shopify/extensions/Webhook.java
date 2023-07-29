package in.succinct.bpp.shopify.extensions;

import com.venky.core.security.Crypt;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.path.Path;
import in.succinct.beckn.Cancellation;
import in.succinct.beckn.Cancellation.CancelledBy;
import in.succinct.beckn.CancellationReasons.CancellationReasonCode;
import in.succinct.beckn.Context;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Message;
import in.succinct.beckn.Option;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.OrderReconStatus;
import in.succinct.beckn.Order.Orders;
import in.succinct.beckn.Order.ReconStatus;
import in.succinct.beckn.Request;
import in.succinct.beckn.SettlementCorrection;
import in.succinct.beckn.Tags;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.core.db.model.Subscriber;
import in.succinct.bpp.core.db.model.rsp.Settlement;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class Webhook implements Extension {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.order_hook",new Webhook());
    }
    @Override
    public void invoke(Object... objects) {
        CommerceAdaptor adaptor = (CommerceAdaptor) objects[0];
        NetworkAdaptor networkAdaptor = (NetworkAdaptor)objects[1];
        Path path = (Path) objects[2];
        if (!(adaptor instanceof ECommerceAdaptor)) {
            return;
        }
        ECommerceAdaptor eCommerceAdaptor = (ECommerceAdaptor) adaptor;
        try {
            hook(eCommerceAdaptor, networkAdaptor,path);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor,Path path) throws Exception{
        String payload = StringUtil.read(path.getInputStream());
        //Validate auth headers from path.getHeader
        String sign = path.getHeader("X-Shopify-Hmac-SHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(
                    eCommerceAdaptor.getConfiguration().get("in.succinct.bpp.shopify.hmac.key").getBytes(StandardCharsets.UTF_8),
                    "HmacSHA256"));
        byte[] hmacbytes = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
        if (!ObjectUtil.equals(Crypt.getInstance().toBase64(hmacbytes),sign)){
            throw new RuntimeException("Webhook - Signature failed!!");
        }
        String event = path.parameter();

        JSONObject eOrder = (JSONObject) JSONValue.parse(payload);
        ShopifyOrder shopifyOrder = new ShopifyOrder(eOrder);

        // Check old order here and if old order is not settled an now it is settled. Also send on_receiver_recon
        String becknTransactionId = eCommerceAdaptor.getBecknTransactionId(shopifyOrder);

        Order lastKnownOrderState = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(eCommerceAdaptor.getSubscriber()).getLastKnownOrder(becknTransactionId);

        Order becknOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder); //Fill all attributes here.
        if (lastKnownOrderState.getReconStatus() == ReconStatus.PAID){
            Request on_receiver_recon = new Request();
            Context context = new Context();
            on_receiver_recon.setContext(context);
            context.setNetworkId(networkAdaptor.getId());
            context.setAction("on_receiver_recon");
            context.setBppId(becknOrder.getReceiverSubscriberId());
            context.setBapId(becknOrder.getCollectorSubscriberId());
            context.setDomain("NTS10");
            context.setTimestamp(new Date());
            context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
            on_receiver_recon.setMessage(new Message());
            Orders orders = new Orders();
            on_receiver_recon.getMessage().setOrders(orders);
            orders.add(becknOrder);

            if (shopifyOrder.isSettled()){
                becknOrder.setOrderReconStatus(OrderReconStatus.FINALE);
                becknOrder.setCounterPartyReconStatus(ReconStatus.PAID);
            }else if (shopifyOrder.getSettledAmount() > 0){
                List<Settlement> settlementList = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(eCommerceAdaptor.getSubscriber()).getSettlements(becknTransactionId);
                Bucket settlementAmountExpected = new Bucket();
                Settlement current = null;
                for (Settlement settlement : settlementList) {
                    settlementAmountExpected.increment(settlement.getExpectedCreditInBank());
                    if (settlement.getSettlementReference().equals(becknOrder.getSettlementReference())){
                        current = settlement;
                    }
                }


                if (current != null) {
                    becknOrder.setCollectionTransactionId(current.getCollectionTxnId());
                }
                becknOrder.setOrderReconStatus(OrderReconStatus.FINALE);
                if (settlementAmountExpected.doubleValue() > shopifyOrder.getSettledAmount()){
                    becknOrder.setCounterPartyReconStatus(ReconStatus.UNDER_PAID);
                    becknOrder.setCounterPartyDiffAmount(settlementAmountExpected.doubleValue()-shopifyOrder.getSettledAmount());
                }else {
                    becknOrder.setCounterPartyReconStatus(ReconStatus.PAID);
                    becknOrder.setCounterPartyDiffAmount(null);
                }
            }
            networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,on_receiver_recon);

        }

        final Request request = new Request();
        request.setMessage(new Message());
        request.setContext(new Context());
        request.getMessage().setOrder(becknOrder);
        Context context = request.getContext();
        context.setBppId(eCommerceAdaptor.getSubscriber().getSubscriberId());
        context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
        context.setTimestamp(new Date());
        context.setAction(event);
        context.setDomain(eCommerceAdaptor.getSubscriber().getDomain());
        shopifyOrder.getNoteAttributes().forEach(na->{
            if (na.getName().startsWith("context.")){
                String key = na.getName().substring("context.".length());
                context.set(key,na.getValue());
            }
        });
        if (path.parameter().equals("on_cancel")){
            if (becknOrder.getCancellation() == null) {
                becknOrder.setCancellation(new Cancellation());
                becknOrder.getCancellation().setCancelledBy(CancelledBy.PROVIDER);
                becknOrder.getCancellation().setSelectedReason(new Option());
                becknOrder.getCancellation().getSelectedReason().setDescriptor(new Descriptor());
                Descriptor descriptor = becknOrder.getCancellation().getSelectedReason().getDescriptor();
                Tags tags = becknOrder.getTags();
                if (tags == null){
                    tags = new Tags();
                    becknOrder.setTags(tags);
                }

                switch (shopifyOrder.getCancelReason()) {
                    case "inventory":
                        descriptor.setCode(CancellationReasonCode.convertor.toString(CancellationReasonCode.ITEM_NOT_AVAILABLE));
                        descriptor.setLongDesc("One or more items in the Order not available");
                        tags.set("cancellation_reason_id",descriptor.getCode());
                        break;
                    case "declined":
                        descriptor.setCode(CancellationReasonCode.convertor.toString(CancellationReasonCode.BUYER_REFUSES_DELIVERY));
                        tags.set("cancellation_reason_id",descriptor.getCode());
                        break;
                    case "other":
                        descriptor.setCode(CancellationReasonCode.convertor.toString(CancellationReasonCode.REJECT_ORDER));
                        descriptor.setLongDesc("Unable to fulfill order!");
                        tags.set("cancellation_reason_id",descriptor.getCode());
                        break;
                }
            }
        }

        //Fill any other attributes needed.
        //Send unsolicited on_status.
        context.setMessageId(UUID.randomUUID().toString());
        networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,request);
    }
}
