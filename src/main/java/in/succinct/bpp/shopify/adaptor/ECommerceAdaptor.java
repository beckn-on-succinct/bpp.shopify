package in.succinct.bpp.shopify.adaptor;

import com.venky.cache.Cache;
import com.venky.cache.UnboundedCache;
import com.venky.core.date.DateUtils;
import com.venky.core.math.DoubleHolder;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.geo.GeoCoordinate;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import com.venky.swf.plugins.collab.db.model.config.City;
import com.venky.swf.plugins.collab.db.model.config.Country;
import com.venky.swf.plugins.collab.db.model.config.State;
import com.venky.swf.plugins.gst.db.model.assets.AssetCode;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.Address;
import in.succinct.beckn.BecknObject;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Billing;
import in.succinct.beckn.BreakUp;
import in.succinct.beckn.BreakUp.BreakUpElement;
import in.succinct.beckn.BreakUp.BreakUpElement.BreakUpCategory;
import in.succinct.beckn.Cancellation;
import in.succinct.beckn.Cancellation.CancelledBy;
import in.succinct.beckn.CancellationReasons.CancellationReasonCode;
import in.succinct.beckn.Contact;
import in.succinct.beckn.Context;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Document;
import in.succinct.beckn.Documents;
import in.succinct.beckn.Error;
import in.succinct.beckn.Error.Type;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.Fulfillment.FulfillmentType;
import in.succinct.beckn.FulfillmentStop;
import in.succinct.beckn.Fulfillments;
import in.succinct.beckn.Images;
import in.succinct.beckn.Item;
import in.succinct.beckn.Item.PackagedCommodity;
import in.succinct.beckn.Item.PrepackagedFood;
import in.succinct.beckn.Item.VeggiesFruits;
import in.succinct.beckn.ItemQuantity;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Message;
import in.succinct.beckn.Option;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.NonUniqueItems;
import in.succinct.beckn.Order.Refund;
import in.succinct.beckn.Order.Refunds;
import in.succinct.beckn.Order.Return;
import in.succinct.beckn.Order.Return.ReturnStatus;
import in.succinct.beckn.Order.Returns;
import in.succinct.beckn.Order.Status;
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payment.CollectedBy;
import in.succinct.beckn.Payment.CommissionType;
import in.succinct.beckn.Payment.NegotiationStatus;
import in.succinct.beckn.Payment.Params;
import in.succinct.beckn.Payment.PaymentStatus;
import in.succinct.beckn.Payment.PaymentType;
import in.succinct.beckn.Payment.SettlementBasis;
import in.succinct.beckn.Person;
import in.succinct.beckn.Price;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.Quote;
import in.succinct.beckn.Request;
import in.succinct.beckn.ReturnReasons.ReturnReasonCode;
import in.succinct.beckn.ReturnReasons.ReturnRejectReason;
import in.succinct.beckn.SellerException;
import in.succinct.beckn.SellerException.CancellationNotPossible;
import in.succinct.beckn.SellerException.GenericBusinessError;
import in.succinct.beckn.SellerException.ItemQuantityUnavailable;
import in.succinct.beckn.SellerException.OrderConfirmFailure;
import in.succinct.beckn.SellerException.UpdationNotPossible;
import in.succinct.beckn.SettlementDetail;
import in.succinct.beckn.SettlementDetail.SettlementCounterparty;
import in.succinct.beckn.SettlementDetail.SettlementPhase;
import in.succinct.beckn.SettlementDetail.SettlementType;
import in.succinct.beckn.SettlementDetails;
import in.succinct.beckn.TagGroup;
import in.succinct.beckn.TagGroups;
import in.succinct.beckn.Time;
import in.succinct.beckn.Time.Range;
import in.succinct.beckn.User;
import in.succinct.bpp.core.adaptor.TimeSensitiveCache;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper.Entity;
import in.succinct.bpp.core.adaptor.fulfillment.FulfillmentStatusAdaptor;
import in.succinct.bpp.core.adaptor.fulfillment.FulfillmentStatusAdaptor.FulfillmentStatusAudit;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.core.db.model.ProviderConfig.Serviceability;
import in.succinct.bpp.search.adaptor.SearchAdaptor;
import in.succinct.bpp.shopify.adaptor.ECommerceSDK.Page;
import in.succinct.bpp.shopify.model.ProductImages;
import in.succinct.bpp.shopify.model.ProductImages.ProductImage;
import in.succinct.bpp.shopify.model.Products;
import in.succinct.bpp.shopify.model.Products.InventoryItem;
import in.succinct.bpp.shopify.model.Products.InventoryItems;
import in.succinct.bpp.shopify.model.Products.InventoryLevel;
import in.succinct.bpp.shopify.model.Products.InventoryLevels;
import in.succinct.bpp.shopify.model.Products.Metafields;
import in.succinct.bpp.shopify.model.Products.Product;
import in.succinct.bpp.shopify.model.Products.ProductVariant;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import in.succinct.bpp.shopify.model.ShopifyOrder.LineItem;
import in.succinct.bpp.shopify.model.ShopifyOrder.LineItems;
import in.succinct.bpp.shopify.model.ShopifyOrder.NoteAttributes;
import in.succinct.bpp.shopify.model.ShopifyOrder.ShippingLine;
import in.succinct.bpp.shopify.model.ShopifyOrder.ShopifyRefund;
import in.succinct.bpp.shopify.model.ShopifyOrder.ShopifyRefund.RefundLineItem;
import in.succinct.bpp.shopify.model.ShopifyOrder.ShopifyRefund.RefundLineItems;
import in.succinct.bpp.shopify.model.ShopifyOrder.TaxLine;
import in.succinct.bpp.shopify.model.ShopifyOrder.TaxLines;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transaction;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transactions;
import in.succinct.bpp.shopify.model.Store;
import in.succinct.json.JSONAwareWrapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ECommerceAdaptor extends SearchAdaptor {
    final ECommerceSDK helper;
    final TimeSensitiveCache cache = new TimeSensitiveCache(Duration.ofDays(1));

    @Override
    public void clearCache(){
        cache.clear();
    }

    public ECommerceAdaptor(Map<String, String> configuration, Subscriber subscriber) {
        super(configuration, subscriber);
        this.helper = new ECommerceSDK(this);
        getProviderConfig().getSupportContact().setEmail(getSupportEmail());
        getProviderConfig().setLocation(getProviderLocations().get(0));
    }


    public String getBecknTransactionId(ShopifyOrder draftOrder){
        return draftOrder.getContext().getTransactionId();
    }


    @Override
    public void init(Request request, Request reply) {

        ShopifyOrder shopifyOrder = new ShopifyOrder();
        Order bo = request.getMessage().getOrder();
        fixFulfillment(request.getContext(), bo);
        fixLocation(bo);
        Fulfillment f = bo.getFulfillment();
        Location storeLocation = bo.getProviderLocation();

        Serviceability serviceability = f.getEnd() == null ? null : getProviderConfig().getServiceability(f.getType(),f.getEnd(),storeLocation);
        if (serviceability != null && !serviceability.isServiceable()){
            throw serviceability.getReason();
        }

        shopifyOrder.setId(LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(request.getContext().getTransactionId()));

        shopifyOrder.setCurrency("INR");
        shopifyOrder.setSourceName("beckn");
        shopifyOrder.setName("beckn-" + request.getContext().getTransactionId());
        shopifyOrder.setNoteAttributes(new NoteAttributes());

        for (String key : new String[]{"bap_id", "bap_uri", "domain", "transaction_id", "city", "country", "core_version" , "ttl" }) {
            TagGroup meta = new TagGroup();
            meta.setName(String.format("context.%s", key));
            meta.setValue(request.getContext().get(key));
            shopifyOrder.getNoteAttributes().add(meta);
        }

        if (!ObjectUtil.isVoid(shopifyOrder.getId())) {
            delete(shopifyOrder);
        }

        setShipping( bo.getFulfillment(), shopifyOrder);

        if (bo.getBilling() == null){
            bo.setBilling(new Billing());
        }
        if (bo.getBilling().getAddress() == null){
            bo.getBilling().setAddress(bo.getFulfillment().getEnd().getLocation().getAddress());
        }
        if (bo.getBilling().getName() == null){
            bo.getBilling().setName(bo.getBilling().getAddress().getName());
        }

        setBilling( bo.getBilling(),shopifyOrder);
        Bucket totalPrice = new Bucket();
        Bucket tax = new Bucket();
        shopifyOrder.setLocationId(Long.parseLong(BecknIdHelper.getLocalUniqueId(getProviderConfig().getLocation().getId(),Entity.provider_location)));

        if (serviceability != null) {
            ShippingLine shippingLine = new ShippingLine();
            shippingLine.setTitle("Standard");
            shippingLine.setPrice(serviceability.getCharges());
            shippingLine.setCode("Local Delivery");
            shippingLine.setPhone(shopifyOrder.getShippingAddress().getPhone());
            shippingLine.setSource("shopify");
            shippingLine.setTaxLines(new TaxLines());

            TaxLine taxLine = new TaxLine();
            taxLine.setRate(0.18);
            taxLine.setTitle("IGST");

            double factor = isTaxIncludedInPrice() ? (taxLine.getRate()/(1 + taxLine.getRate())) : taxLine.getRate();
            double taxIncluded = serviceability.getCharges() * factor;


            taxLine.setPrice(taxIncluded);


            shippingLine.getTaxLines().add(taxLine);

            shopifyOrder.setShippingLine(shippingLine);

            totalPrice.increment(serviceability.getCharges());
            tax.increment(taxIncluded);
        }

        shopifyOrder.setTaxesIncluded(isTaxIncludedInPrice());

        if (bo.getItems() != null) {
            bo.getItems().forEach(boItem -> {
                in.succinct.bpp.search.db.model.Item dbItem = getItem(boItem.getId());
                if (dbItem == null){
                    return;
                }

                Item refreshedBoItem = new Item(dbItem.getObjectJson());
                JSONObject inspectQuantity = (JSONObject) boItem.getInner().get("quantity");
                if (inspectQuantity.containsKey("count")){
                    refreshedBoItem.setQuantity(boItem.getQuantity());
                }else {
                    refreshedBoItem.setItemQuantity(boItem.getItemQuantity());
                }

                if (!dbItem.isActive()){
                    ItemQuantityUnavailable ex = new SellerException.ItemQuantityUnavailable() ;
                    Error error = new Error();
                    error.setCode(ex.getErrorCode());
                    error.setMessage(ex.getMessage());
                    error.setType(Type.DOMAIN_ERROR);
                    reply.setError(error);
                    Quantity zero = new Quantity();
                    zero.setCount(0);
                    if (inspectQuantity.containsKey("count")){
                        refreshedBoItem.setQuantity(zero);
                        boItem.setQuantity(refreshedBoItem.getQuantity());
                    }else {
                        refreshedBoItem.setItemQuantity(new ItemQuantity());
                        refreshedBoItem.getItemQuantity().setAvailable(zero);
                        refreshedBoItem.getItemQuantity().setMaximum(zero);
                        refreshedBoItem.getItemQuantity().setSelected(zero);
                        boItem.setItemQuantity(refreshedBoItem.getItemQuantity());
                    }
                }else {
                    LineItem lineItem = addItem(shopifyOrder, refreshedBoItem);
                    lineItem.setTaxLines(new TaxLines());

                    double linePrice = refreshedBoItem.getPrice().getValue() * lineItem.getQuantity();
                double taxRate = doubleTypeConverter.valueOf(refreshedBoItem.getTaxRate())/100.0;
                    double lineTax = linePrice * (isTaxIncludedInPrice() ? taxRate / (1.0 + taxRate) : taxRate);
                    totalPrice.increment(linePrice);
                    tax.increment(lineTax);

                    String[] taxHeads = new String[]{"IGST"};
                    if (ObjectUtil.equals(getProviderConfig().getLocation().getAddress().getState(), shopifyOrder.getShippingAddress().getAddress().getState())) {
                        taxHeads = new String[]{"SGST", "CGST"};
                    }
                    int numHeads = taxHeads.length;
                    for (String head : taxHeads) {
                        TaxLine taxLine = new TaxLine();
                        taxLine.setPrice(lineTax / numHeads);
                        taxLine.setRate(taxRate / numHeads);
                        taxLine.setTitle(head);
                        lineItem.getTaxLines().add(taxLine);

                    }
                }

            });
        }
        shopifyOrder.setTotalTax(tax.doubleValue());

        if (Config.instance().isDevelopmentEnvironment()){
            shopifyOrder.setTest(true);
            shopifyOrder.setTransactions(new Transactions());
            Transactions transactions = shopifyOrder.getTransactions();
            transactions.add(new Transaction(){{
                setTest(true);
                setKind("authorization");
                setStatus("success");
                setAmount(totalPrice.intValue());
            }});
        }

        LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).sync(request.getContext().getTransactionId(),bo);
        Order order = bo;
        if (!shopifyOrder.getLineItems().isEmpty()){
            order =  saveDraftOrder(shopifyOrder);
            if (bo.getCreatedAt() != null ){
                order.setCreatedAt(bo.getCreatedAt());
            }
        }

        reply.setMessage(new Message());
        reply.getMessage().setOrder(order);
    }

    private void delete(ShopifyOrder draftOrder) {
        helper.delete(String.format("/orders/%s.json",draftOrder.getId()) , new JSONObject());
        draftOrder.rm("id");
    }

    private in.succinct.bpp.search.db.model.Item getItem(String objectId) {

        Select select = new Select().from(in.succinct.bpp.search.db.model.Item.class);
        List<in.succinct.bpp.search.db.model.Item> dbItems = select.where(new Expression(select.getPool(), Conjunction.AND).
                add(new Expression(select.getPool(), "APPLICATION_ID", Operator.EQ, getApplication().getId())).
                add(new Expression(select.getPool(), "OBJECT_ID", Operator.EQ, objectId))).execute(1);

        return dbItems.isEmpty() ? null : dbItems.get(0);
    }

    private LineItem addItem(ShopifyOrder draftOrder, Item item) {
        LineItems line_items = draftOrder.getLineItems();
        if (line_items == null) {
            line_items = new LineItems();
            draftOrder.setLineItems(line_items);
        }
        LineItem lineItem = new LineItem();
        lineItem.setVariantId(Long.parseLong(BecknIdHelper.getLocalUniqueId(item.getId(), BecknIdHelper.Entity.item)));

        JSONObject inspectQuantity = (JSONObject) item.getInner().get("quantity");
        if (inspectQuantity.containsKey("count")){
            lineItem.setQuantity(item.getQuantity().getCount());
        }else if (inspectQuantity.containsKey("allocated")){
            lineItem.setQuantity(item.getItemQuantity().getAllocated().getCount());
        }

        lineItem.setProductId(doubleTypeConverter.valueOf(item.getTags().get("product_id")).longValue());
        lineItem.setRequiresShipping(true);
        lineItem.setTaxable(doubleTypeConverter.valueOf(item.getTaxRate()) > 0);
        line_items.add(lineItem);
        return lineItem;
    }

    private final TypeConverter<Double> doubleTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter();
    private final TypeConverter<Boolean> booleanTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(boolean.class).getTypeConverter();

    public void setBilling(Billing source, ShopifyOrder target) {

        if (source == null) {
            return;
        }
        ShopifyOrder.Address billing = new ShopifyOrder.Address();
        target.setBillingAddress(billing);

        String[] parts = source.getName().split(" ");
        billing.setName(source.getName());
        billing.setFirstName(parts[0]);
        billing.setLastName(source.getName().substring(parts[0].length()));
        if (source.getAddress() != null) {
            billing.setAddress1(source.getAddress().getDoor() + "," + source.getAddress().getBuilding());
            billing.setAddress2(source.getAddress().getStreet() + "," + source.getAddress().getLocality());

            Country country = Country.findByName(source.getAddress().getCountry());
            State state = State.findByCountryAndName(country.getId(), source.getAddress().getState());
            City city = City.findByStateAndName(state.getId(), source.getAddress().getCity());

            billing.setCity(city.getName());
            billing.setProvince(city.getState().getName());
            billing.setProvinceCode(city.getState().getCode());
            billing.setCountryCode(city.getState().getCountry().getIsoCode2());
            billing.setCountry(country.getName());
            billing.setZip(source.getAddress().getAreaCode());
        }


        billing.setPhone(source.getPhone());

    }

    public void setShipping(Fulfillment source, ShopifyOrder target) {
        if (source == null) {
            return;
        }
        ShopifyOrder.Address shipping = new ShopifyOrder.Address();
        target.setShippingAddress(shipping);

        User user = source.getCustomer();
        Address address = source.getEnd().getLocation().getAddress();
        if (user == null && address != null) {
            user = new User();
            user.setPerson(new Person());
            user.getPerson().setName(address.getName());
        }

        if (user != null) {
            String[] parts = user.getPerson().getName().split(" ");
            shipping.setName(user.getPerson().getName());
            shipping.setFirstName(parts[0]);
            shipping.setLastName(user.getPerson().getName().substring(parts[0].length()));
        }


        Contact contact = source.getEnd().getContact();
        GeoCoordinate gps = source.getEnd().getLocation().getGps();

        if (address != null){
            if (address.getCountry() == null){
                address.setCountry(getProviderConfig().getLocation().getAddress().getCountry());
            }
            Country country = Country.findByName(address.getCountry());
            State state = State.findByCountryAndName(country.getId(), address.getState());
            City city = City.findByStateAndName(state.getId(), address.getCity());

            String [] lines = address.getAddressLines();
            shipping.setAddress1(lines[0]);
            shipping.setAddress2(lines[1]);

            shipping.setCity(city.getName());
            shipping.setProvinceCode(state.getCode());
            shipping.setProvince(state.getName());
            shipping.setZip(address.getAreaCode());
            shipping.setCountryCode(country.getIsoCode2());
            shipping.setCountry(country.getName());
        }
        //shipping.put("email",contact.getEmail());
        shipping.setPhone(contact.getPhone());
        target.setPhone(contact.getPhone());
        target.setEmail(contact.getEmail());
        if (gps != null) {
            shipping.setLatitude(gps.getLat().doubleValue());
            shipping.setLongitude(gps.getLng().doubleValue());
        }
    }

    private Order saveDraftOrder(ShopifyOrder draftOrder) {
        JSONObject dro = new JSONObject();
        dro.put("order",draftOrder.getInner());

        JSONObject outOrder = helper.post("/orders.json", dro);
        ShopifyOrder oDraftOrder = new ShopifyOrder((JSONObject) outOrder.get("order"));
        return getBecknOrder(oDraftOrder);

    }

    @Override
    public Order confirmDraftOrder(Order inOrder) {
        String shopifyOrderId = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(inOrder);

        if (Config.instance().isDevelopmentEnvironment() ) {
            if (inOrder.getPayment().getStatus() == PaymentStatus.PAID) {
                JSONObject transactionsJs = helper.get(String.format("/orders/%s/transactions.json", shopifyOrderId), new JSONObject());
                Transactions transactions = new Transactions((JSONArray) transactionsJs.get("transactions"));

                Transaction transaction = transactions.get(0);
                transaction.setParentId(Long.parseLong(transaction.getId()));
                transaction.setKind("capture");
                transaction.rm("id");
                JSONObject params = new JSONObject();
                params.put("transaction", transaction.getInner());

                JSONObject transactionJs = helper.post(String.format("/orders/%s/transactions.json", shopifyOrderId), params);
                transaction = new Transaction((JSONObject) transactionJs.get("transaction"));
                if (!transaction.getStatus().equals("success")) {
                    throw new OrderConfirmFailure("Payment failure");
                }
            }else if (inOrder.getPayment().getCollectedBy() == CollectedBy.BPP){
                JSONObject metaField = new JSONObject();
                metaField.put("key","cod");
                metaField.put("namespace","ondc");
                metaField.put("ownerId","gid://shopify/Order/"+shopifyOrderId);
                metaField.put("value","true");

                JSONArray metaFields = new JSONArray();
                metaFields.add(metaField);

                JSONObject parameter = new JSONObject();
                parameter.put("query","mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) { metafieldsSet(metafields: $metafields) { metafields { key namespace value createdAt updatedAt } userErrors { field message code } } }");
                parameter.put("variables",new JSONObject());
                ((JSONObject)parameter.get("variables")).put("metafields",metaFields);
                JSONObject outMeta = helper.post("graphql.json",parameter);
                Metafields outMetaFields = new Metafields((JSONArray) (((JSONObject)((JSONObject)outMeta.get("data")).get("metafieldsSet")).get("metafields")));
                if (!outMetaFields.get(0).getValue().equals("true")){
                     throw new OrderConfirmFailure("Cod not supported right now");
                }
            }else {
                throw new OrderConfirmFailure("COD can be collected only by BPP");
            }
        }


        return getBecknOrder(getShopifyOrder(shopifyOrderId));
    }


    @Override
    public String getTrackingUrl(Order order) {
        String trackUrl = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getTrackingUrl(order);
        if (trackUrl != null){
            return trackUrl;
        }

        JSONObject response = helper.get(String.format("/orders/%s.json", LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(order)), new JSONObject());
        ShopifyOrder eCommerceOrder = new ShopifyOrder((JSONObject) response.get("order"));
        eCommerceOrder.loadMetaFields(helper);
        if (getFulfillmentStatusAdaptor() != null){
            trackUrl = getFulfillmentStatusAdaptor().getTrackingUrl(StringUtil.valueOf(eCommerceOrder.getOrderNumber()));
        }else if (eCommerceOrder.getTrackingUrl() != null) {
            trackUrl = eCommerceOrder.getTrackingUrl();
        }/*else {
            JSONObject params = new JSONObject();
            params.put("fields", "tracking_urls");

            JSONObject fulfillmentJson = helper.get(String.format("/orders/%s/fulfillments.json", LocalOrderSynchronizer.getInstance().getLocalOrderId(order)), params);
            Fulfillments fulfillments = new Fulfillments((JSONArray) fulfillmentJson.get("fulfillments"));
            String url = null;
            if (fulfillments.size() > 0) {
                ShopifyOrder.Fulfillment fulfillment = fulfillments.get(0);
                BecknStrings urls = fulfillment.getTrackingUrls();
                if (urls.size() > 0) {
                    url = urls.get(0);
                }
            }
            trackUrl = url;
        }*/
        if (trackUrl != null){
            LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).setTrackingUrl(getBecknTransactionId(eCommerceOrder),trackUrl);
        }
        return trackUrl;
    }

    @Override

    public Order cancel(Order order ) {

        JSONObject params = new JSONObject();
        params.put("reason", "customer");

        JSONObject response = helper.post(String.format("/orders/%s/cancel.json", LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(order)), params);
        String error = (String)response.get("error");
        if (!ObjectUtil.isVoid(error)) {
            throw new CancellationNotPossible(error);
        }
        ShopifyOrder eCommerceOrder = new ShopifyOrder((JSONObject) response.get("order"));
        return getBecknOrder(eCommerceOrder);
    }

    @Override
    public Order getStatus(Order order) {
        return getBecknOrder(getShopifyOrder(order));
    }

    public ShopifyOrder getShopifyOrder(Order order){
        /* Take status message and fill response with on_status message */
        return getShopifyOrder(LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(order));
    }
    public ShopifyOrder getShopifyOrder(String shopifyOrderId){
        /* Take status message and fill response with on_status message */
        if (shopifyOrderId.startsWith("gid:")){
            shopifyOrderId = shopifyOrderId.substring(shopifyOrderId.lastIndexOf("/")+1);
        }
        JSONObject response = helper.get(String.format("/orders/%s.json", shopifyOrderId), new JSONObject());
        return new ShopifyOrder((JSONObject) response.get("order"));
    }

    public String getSupportEmail() {
        return getStore().getSupportEmail();
    }


    @Override
    public Locations getProviderLocations() {
        return cache.get(Locations.class, () -> {
            Locations locations = new Locations();
            Store onlineStore = getStore();

            JSONObject response = helper.get("/locations.json", new JSONObject());
                JSONArray stores = (JSONArray) response.get("locations");
            stores.forEach(o -> {
                JSONObject store = (JSONObject) o;

                if (!booleanTypeConverter.valueOf(store.get("active"))) {
                    return;
                }
                if (!ObjectUtil.equals(onlineStore.getPrimaryLocationId() ,store.get("id"))){
                    return;
                }
                Location location = new Location();
                location.setId(BecknIdHelper.getBecknId(StringUtil.valueOf(store.get("id")), getSubscriber(), Entity.provider_location));
                location.setAddress(new Address());
                location.getAddress().setName(getProviderConfig().getStoreName());
                //  location.getAddress().setName((String) store.get("name"));
                location.getAddress().setStreet((String)store.get("address1"));
                location.getAddress().setLocality((String)store.get("address2"));
                location.getAddress().setCity((String) store.get("city"));
                location.getAddress().setPinCode((String) store.get("zip"));
                location.getAddress().setCountry((String) store.get("country"));
                location.getAddress().setState((String) store.get("province"));
                /* TODO
                location.setCity(new in.succinct.beckn.City());
                location.getCity().setCode(location.getAddress().getCity());
                location.setCountry(new in.succinct.beckn.Country());
                location.getCountry().setCode(location.getAddress().getCountry());
                */
                location.setTime(getProviderConfig().getTime());
                location.getTime().setLabel("enable");
                location.getTime().setTimestamp(new Date());
                location.setDescriptor(new Descriptor());
                location.getDescriptor().setName(location.getAddress().getName());

                JSONObject meta = helper.get(String.format("/locations/%s/metafields.json",StringUtil.valueOf(store.get("id"))),new JSONObject());
                Map<String,String> locationMeta = new HashMap<>();
                JSONArray metafields = (JSONArray) meta.get("metafields");
                for (Object metafield : metafields){
                    JSONObject m = (JSONObject) metafield;
                    locationMeta.put((String)m.get("key"),String.valueOf(m.get("value")));
                }
                location.setGps(new GeoCoordinate(new BigDecimal(locationMeta.get("lat")),new BigDecimal(locationMeta.get("lng"))));
                locations.add(location);
            });
            return locations;
        });
    }

    @Override
    public Items getItems(){
        return cache.get(Items.class, () -> getItems(getPublishedProducts()));
    }

    public Items getItems(Set<String> specificProductIds) {
        Items items = new Items();
        InventoryLevels levels = getInventoryLevels();
        //Set<String> specificProductIds = getPublishedProducts();
        Cache<Long, List<Long>> inventoryLocations = new Cache<>(0, 0) {
            @Override
            protected List<Long> getValue(Long aLong) {
                return new ArrayList<>();
            }
        };
        levels.forEach(level -> inventoryLocations.get(level.getInventoryItemId()).add(level.getLocationId()));

        Products products = getProducts(specificProductIds);
        products.forEach(product -> {
            for (ProductVariant productVariant : product.getProductVariants()) {
                inventoryLocations.get(productVariant.getInventoryItemId()); // Just load keys
            }
        });

        InventoryItems inventoryItems = getInventoryItems(inventoryLocations.keySet());

        Map<String, Double> taxRateMap = getTaxRateMap();

        for (Product product : products) {

            for (ProductVariant variant : product.getProductVariants()) {
                InventoryItem inventoryItem = inventoryItems.get(StringUtil.valueOf(variant.getInventoryItemId()));
                if (inventoryItem == null){
                    continue;
                }
                if (!inventoryItem.isTracked() && inventoryLocations.get(variant.getInventoryItemId()).isEmpty()){
                    inventoryLocations.get(variant.getInventoryItemId()).add(getStore().getPrimaryLocationId());
                }

                inventoryLocations.get(variant.getInventoryItemId()).forEach(location_id -> {
                    Item item = new Item();
                    item.setId(BecknIdHelper.getBecknId(variant.getId(), getSubscriber(), Entity.item));

                    Descriptor descriptor = new Descriptor();
                    item.setDescriptor(descriptor);
                    if (!ObjectUtil.isVoid(variant.getBarCode())) {
                        descriptor.setCode(variant.getBarCode());
                    }else {
                        descriptor.setCode(product.getTitle());
                    }

                    if (ObjectUtil.equals(variant.getTitle(),"Default Title")){
                        descriptor.setName(product.getTitle());
                    }else {
                        descriptor.setName(String.format("%s - ( %s )" ,product.getTitle(),variant.getTitle()));
                    }

                    descriptor.setShortDesc(descriptor.getName());
                    descriptor.setLongDesc(descriptor.getName());
                    descriptor.setImages(new Images());
                    ProductImages productImages = product.getImages();

                    if (variant.getImageId() > 0) {
                        ProductImage image = productImages.get(StringUtil.valueOf(variant.getImageId()));
                        if (image != null) {
                            descriptor.getImages().add(image.getSrc());
                        }
                    } else if (!productImages.isEmpty()) {
                        for (Iterator<ProductImage> i = productImages.iterator() ; i.hasNext() && descriptor.getImages().size() < 3; ) {
                            ProductImage image = i.next();
                            descriptor.getImages().add(image.getSrc());
                        }
                    }
                    descriptor.setSymbol(descriptor.getImages().get(0));

                    item.setCategoryId(getProviderConfig().getCategory().getId());

                    for (String tag: product.getTagSet()) {
                        item.setTag("general_attributes",tag,true);
                    }
                    item.setTag("general_attributes","product_id", variant.getProductId());
                    item.setHsnCode(inventoryItem.getHarmonizedSystemCode());
                    item.setTaxRate(taxRateMap.get(inventoryItem.getHarmonizedSystemCode()));
                    if (inventoryItem.getCountryCodeOfOrigin() == null){
                        return;
                    }
                    item.setCountryOfOrigin(Country.findByISO(inventoryItem.getCountryCodeOfOrigin()).getIsoCode());
                    item.setVeg(product.isVeg());



                    item.setLocationId(BecknIdHelper.getBecknId(StringUtil.valueOf(location_id), getSubscriber(), Entity.provider_location));
                    item.setLocationIds(new BecknStrings());
                    item.getLocationIds().add(item.getLocationId());

                    /* Yhis should be abtracted out TODO */


                    if (VeggiesFruits.CategoryNames.contains(getProviderConfig().getCategory().getDescriptor().getName())) {
                        item.setVeggiesFruits(new VeggiesFruits());
                        item.getVeggiesFruits().setNetQuantity(variant.getGrams() + " gms");
                    }else if (PrepackagedFood.CategoryNames.contains(getProviderConfig().getCategory().getDescriptor().getName())) {
                        item.setPrepackagedFood(new PrepackagedFood());
                        item.getPrepackagedFood().setNetQuantity(variant.getGrams() + " gms");
                        item.getPrepackagedFood().setBrandOwnerAddress(getProviderConfig().getLocation().getAddress().flatten());
                        item.getPrepackagedFood().setBrandOwnerName(getProviderConfig().getFulfillmentProviderName());
                        item.getPrepackagedFood().setBrandOwnerFSSAILicenseNo(getProviderConfig().getFssaiRegistrationNumber());
                    }else if (PackagedCommodity.CategoryNames.contains(getProviderConfig().getCategory().getDescriptor().getName())) {
                        item.setPackagedCommodity(new PackagedCommodity());
                        item.getPackagedCommodity().setNetQuantityOrMeasureOfCommodityInPkg(variant.getGrams() + " gms");
                        item.getPackagedCommodity().setManufacturerOrPackerName(getProviderConfig().getFulfillmentProviderName());
                        item.getPackagedCommodity().setManufacturerOrPackerAddress(getProviderConfig().getLocation().getAddress().flatten());
                        item.getPackagedCommodity().setCommonOrGenericNameOfCommodity(product.getProductType());
                        if (inventoryItem.getCountryCodeOfOrigin() != null) {
                            item.getPackagedCommodity().setImportedProductCountryOfOrigin(Country.findByISO(inventoryItem.getCountryCodeOfOrigin()).getIsoCode());
                        }
                        item.getPackagedCommodity().setMonthYearOfManufacturePackingImport("Please refer to the packaging of the product");

                        //For statutory requirements show images.
                    }



                    Price price = new Price();
                    item.setPrice(price);
                    price.setMaximumValue(variant.getMrp());
                    price.setListedValue(variant.getPrice());
                    price.setCurrency(getStore().getCurrency());
                    price.setValue(variant.getPrice());
                    price.setOfferedValue(variant.getPrice());
                    if (doubleTypeConverter.valueOf(price.getMaximumValue()) < doubleTypeConverter.valueOf(price.getValue())){
                        price.setMaximumValue(price.getValue());
                    }
                    price.setCurrency("INR");

                    double taxRate = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter().valueOf(item.getTaxRate())/100.0;
                    Price unitTax = new Price();
                    unitTax.setCurrency("INR");
                    double factor = isTaxIncludedInPrice() ? (taxRate/(1 + taxRate)) : taxRate ;

                    unitTax.setValue(factor * item.getPrice().getValue());
                    unitTax.setListedValue(factor  * item.getPrice().getListedValue());
                    unitTax.setOfferedValue(factor * item.getPrice().getOfferedValue());
                    item.setTax(unitTax);


                    item.setPaymentIds(new BecknStrings());
                    for (Payment payment : getSupportedPaymentCollectionMethods()) {
                        item.getPaymentIds().add(payment.getId()); //Only allow By BAP , ON_ORDER
                    }

                    item.setReturnable(getProviderConfig().isReturnSupported());
                    if (item.isReturnable()){
                        item.setReturnWindow(getProviderConfig().getReturnWindow());
                        item.setSellerPickupReturn(getProviderConfig().isReturnPickupSupported());
                    }else {
                        item.setReturnWindow(Duration.ofDays(0));
                    }

                    item.setCancellable(true);
                    item.setTimeToShip(getProviderConfig().getTimeToShip());
                    item.setAvailableOnCod(getProviderConfig().isCodSupported());

                    item.setContactDetailsConsumerCare(String.format("%s,%s,%s",
                            getProviderConfig().getLocation().getAddress().getName() ,
                            getProviderConfig().getSupportContact().getEmail() ,
                            getProviderConfig().getSupportContact().getPhone()));
                    item.setFulfillmentIds(new BecknStrings());
                    for (Fulfillment fulfillment : getFulfillments()) {
                        item.getFulfillmentIds().add(fulfillment.getId());
                    }


                    items.add(item);

                });
            }
        }
        return items;
    }

    public Map<String,Double> getTaxRateMap(){
        return new Cache<>(0,0) {
            @Override
            protected Double getValue(String taxClass) {
                if (!ObjectUtil.isVoid(taxClass)){
                    AssetCode assetCode = AssetCode.findLike(taxClass,AssetCode.class);
                    if (assetCode != null) {
                        return assetCode.getReflector().getJdbcTypeHelper().getTypeRef(double.class).getTypeConverter().valueOf(assetCode.getGstPct());
                    }
                }
                return 0.0D;
            }
        };
    }

    private Set<String> getPublishedProducts(){
        return cache.get(PublishedProducts.class,()->{
            JSONObject input = new JSONObject();
            input.put("limit", 1000);


            PublishedProducts ids = new PublishedProducts();

            Page<JSONObject> productsPage = new Page<>();
            productsPage.next = "/product_listings/product_ids.json";
            while (productsPage.next != null) {
                productsPage = helper.getPage(productsPage.next, input);
                if (productsPage.data == null){
                    break;
                }

                JSONArray productIds = ((JSONArray)productsPage.data.get("product_ids"));
                for (Object id : productIds){
                    ids.add(id.toString());
                }
            }
            return ids;
        });
    }

    public JSONObject getReturn(String returnId) {
        JSONObject o = helper.graphql(String.format("{ return(id: \"%s\") { order { id } decline { reason note }  status reverseFulfillmentOrders(first : 10) {edges { node { id status reverseDeliveries(first:10) { edges { node { id  deliverable { ... on ReverseDeliveryShippingDeliverable{ tracking { carrierName number url }    }  }  } }  }} }}}}"
                ,returnId));
        return (JSONObject)(((JSONObject)o.get("data")).get("return"));
    }
    
    public static class ReturnDecline extends BecknObject {
        public ReturnDecline() {
        }

        public ReturnDecline(JSONObject object) {
            super(object);
        }

        public String getNote(){
            return get("note");
        }
        
        public ReturnDeclineReason getReturnDeclineReason(){
            return getEnum(ReturnDeclineReason.class, "reason");
        }
        public void setReturnDeclineReason(ReturnDeclineReason return_decline_reason){
            setEnum("reason",return_decline_reason);
        }

    }

    public enum ReturnDeclineReason {
        FINAL_SALE,
        RETURN_PERIOD_ENDED,
        OTHER,
    }
    public static class PublishedProducts extends HashSet<String> {}

    private InventoryLevels getInventoryLevels() {
        return cache.get(InventoryLevels.class,()->{
            StringBuilder locationIds = new StringBuilder();
            for (Iterator<Location> i = getProviderLocations().iterator(); i.hasNext(); ) {
                Location location = i.next();
                locationIds.append(BecknIdHelper.getLocalUniqueId(location.getId(),Entity.provider_location));
                if (i.hasNext()) {
                    locationIds.append(",");
                }
            }
            JSONObject input = new JSONObject();
            input.put("location_ids", locationIds.toString());
            input.put("limit", 250);

            InventoryLevels levels = new InventoryLevels();
            Page<JSONObject> pageInventoryLevels = new Page<>();
            pageInventoryLevels.next = "/inventory_levels.json";
            while (pageInventoryLevels.next != null) {
                pageInventoryLevels = helper.getPage(pageInventoryLevels.next, input);
                input.remove("location_ids");
                if (pageInventoryLevels.data != null ){
                    JSONArray inventoryLevels = (JSONArray) pageInventoryLevels.data.get("inventory_levels");
                    inventoryLevels.forEach(o -> levels.add(new InventoryLevel((JSONObject) o)));
                }else {
                    break;
                }
            }
            return levels;
        });
    }
    private Products getProducts(Set<String> specificProductIds){

        StringBuilder idsQuery = new StringBuilder();
        for (String id: specificProductIds){
            if (idsQuery.length() >0){
                idsQuery.append(",");
            }
            idsQuery.append(id);
        }
        JSONObject input = new JSONObject();
        input.put("limit", 250);
        input.put("ids",idsQuery);

        Products products = new Products();

        Page<JSONObject> productsPage = new Page<>();
        productsPage.next = "/products.json";
        while (productsPage.next != null) {
            productsPage = helper.getPage(productsPage.next, input);
            input.remove("ids");
            if (productsPage.data == null){
                break;
            }
            Products productsInPage = new Products((JSONArray) productsPage.data.get("products"));
            for (Product product : productsInPage) {
                if (!product.isActive()) {
                    continue;
                }
                if (product.getProductVariants() == null){
                    continue;
                }
                products.add(product);
            }
        }
        return products;
    }

    private InventoryItems getInventoryItems(Set<Long> ids){
        if (ids.isEmpty()){
            return new InventoryItems();
        }
        return cache.get(InventoryItems.class,()->{
            StringBuilder inventoryItemIds = new StringBuilder();
            for (Iterator <Long> i = ids.iterator() ; i .hasNext() ; ) {
                inventoryItemIds.append(i.next());
                if (i.hasNext()){
                    inventoryItemIds.append(",");
                }
            }
            JSONObject input = new JSONObject();
            input.put("ids",inventoryItemIds.toString());
            input.put("limit" , 250);

            InventoryItems inventoryItems = new InventoryItems();
            Page<JSONObject> pageInventoryItems= new Page<>();
            pageInventoryItems.next= "/inventory_items.json" ;
            while (pageInventoryItems.next != null){
                pageInventoryItems = helper.getPage(pageInventoryItems.next,input);
                input.remove("ids");
                if (pageInventoryItems.data != null) {
                    JSONArray inventoryItemsArray = (JSONArray) pageInventoryItems.data.get("inventory_items");
                    inventoryItemsArray.forEach(o -> inventoryItems.add(new InventoryItem((JSONObject) o)));
                }else{
                    break;
                }
            }
            return inventoryItems;
        });
    }


    public Store getStore(){
        return cache.get(Store.class,()->{
            JSONObject storeJson = helper.get("/shop.json", new JSONObject());
            return new Store((JSONObject) storeJson.get("shop"));
        });
    }



    @Override
    public boolean isTaxIncludedInPrice() {
        return getStore().isTaxesIncluded();
    }


    //Get Beckn Order

    public List<FulfillmentStatusAudit> getStatusAudit (Order order) {
        JSONObject response = helper.get(String.format("/orders/%s.json", LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(order)), new JSONObject());
        ShopifyOrder eCommerceOrder = new ShopifyOrder((JSONObject) response.get("order"));
        FulfillmentStatusAdaptor adaptor = getFulfillmentStatusAdaptor() ;
        String transactionId = getBecknTransactionId(eCommerceOrder);

        if (adaptor != null){
            List<FulfillmentStatusAudit> audits = adaptor.getStatusAudit(String.valueOf(eCommerceOrder.getOrderNumber()));
            for (Iterator<FulfillmentStatusAudit> i = audits.iterator(); i.hasNext() ; ){
                FulfillmentStatusAudit audit = i.next();
                LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).setFulfillmentStatusReachedAt(transactionId,audit.getFulfillmentStatus(),audit.getDate(),!i.hasNext());
            }
        }
        return LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getFulfillmentStatusAudit(transactionId);

    }

    public Order getBecknOrder(ShopifyOrder eCommerceOrder) {
        eCommerceOrder.loadMetaFields(helper);

        String transactionId = getBecknTransactionId(eCommerceOrder);
        LocalOrderSynchronizer localOrderSynchronizer = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber());
        localOrderSynchronizer.setLocalOrderId(transactionId,eCommerceOrder.getId());
        Order lastReturnedOrderJson = localOrderSynchronizer.getLastKnownOrder(transactionId,true);

        Order order = new Order();
        order.update(lastReturnedOrderJson);
        //order.setFulfillments(new in.succinct.beckn.Fulfillments());

        if (order.getPayment() == null){
            order.setPayment(new Payment());
        }

        Fulfillment fulfillment = order.getPrimaryFulfillment();
        if (fulfillment != null){
            order.setFulfillment(fulfillment);
        }

        if (fulfillment.getId() == null) {
            String fulfillmentId = "fulfillment/" + fulfillment.getType() + "/" + transactionId;
            fulfillment.setId(fulfillmentId);
        }

        if (ObjectUtil.isVoid(order.getFulfillment().getType())) {
            fulfillment.setType(FulfillmentType.home_delivery);
        }

        Date updatedAt = eCommerceOrder.getUpdatedAt();

        localOrderSynchronizer.setFulfillmentStatusReachedAt(transactionId,eCommerceOrder.getFulfillmentStatus(),updatedAt,false);
        localOrderSynchronizer.setStatusReachedAt(transactionId,eCommerceOrder.getStatus(),updatedAt,false);


        List<FulfillmentStatusAudit> fulfillmentStatusAudits = localOrderSynchronizer.getFulfillmentStatusAudit(transactionId);
        Map<FulfillmentStatus,Date> auditMap = new HashMap<>();

        for (FulfillmentStatusAudit a : fulfillmentStatusAudits){
            auditMap.put(a.getFulfillmentStatus(),a.getDate());
            if (updatedAt.compareTo(a.getDate()) < 0){
                updatedAt = a.getDate();
            }
        }

        order.setUpdatedAt(DateUtils.max(updatedAt,order.getUpdatedAt()));

        Map<String,Map<String,Item>> returnedItems = getReturnedItems(order);
        Map<String,Return> refundReturnMap = new HashMap<>();
        for (Return aReturn : order.getReturns()) {
            if (!ObjectUtil.isVoid(aReturn.getRefundId())) {
                refundReturnMap.put(aReturn.getRefundId(), aReturn);
            }
        }

        Map<String,RefundLineItems> refundLineItemsMap = new UnboundedCache<>() {
            @Override
            protected RefundLineItems getValue(String lineItemId) {
                return new RefundLineItems();
            }
        };
        Bucket orderRefundAmount = new Bucket();
        for (ShopifyRefund refund : eCommerceOrder.getRefunds()) {
            for (RefundLineItem refundLineItem : refund.getRefundLineItems()) {
                refundLineItem.setRefundId(refund.getId());
                LineItem lineItem = refundLineItem.getLineItem();
                orderRefundAmount.increment(lineItem.getPrice() * refundLineItem.getQuantity());
                if (refundReturnMap.containsKey(refund.getId())) {
                    refundLineItem.setReturnId(refundReturnMap.get(refund.getId()).getId());
                }
                refundLineItemsMap.get(lineItem.getId()).add(refundLineItem);
            }
        }


        order.setState(eCommerceOrder.getStatus());
        order.setItems(new NonUniqueItems());

        initializeRefundFulfillments(order,eCommerceOrder);
        loadShippingQuoteBreakup(eCommerceOrder,order,orderRefundAmount);

        setPayment(order.getPayment(),eCommerceOrder,orderRefundAmount.doubleValue());

        Double feeAmount = order.getPayment().getBuyerAppFinderFeeAmount();
        if (feeAmount != null) {
            if (order.getPayment().getBuyerAppFinderFeeType() == CommissionType.Percent) {
                if (feeAmount > getProviderConfig().getMaxAllowedCommissionPercent()) {
                    throw new GenericBusinessError("Max commission percent exceeded");
                }
            } else {
                double pct = new DoubleHolder(100.0 * feeAmount / order.getPayment().getParams().getAmount(), 2).getHeldDouble().doubleValue();
                if (pct > getProviderConfig().getMaxAllowedCommissionPercent()) {
                    throw new GenericBusinessError("Max commission percent exceeded");
                }
            }
        }

        setBilling(order,eCommerceOrder);
        //order.setId(meta.getBapOrderId());
        loadInvoice(order,eCommerceOrder);

        eCommerceOrder.getLineItems().forEach(lineItem -> {
            //item.setFulfillmentId(lastReturnedOrderJson.getFulfillment().getId());
            final Item unitItem = createItemFromECommerceLineItem(lineItem,refundLineItemsMap, order.getReturns(), order.getRefunds(),order.getFulfillment().getId());
            if (unitItem.getQuantity().getCount() > 0 ) {
                order.getItems().add(unitItem);
            }
            addToQuote(order.getQuote(),unitItem);

            String itemId = unitItem.getId();
            returnedItems.get(itemId).forEach((returnId,ri)->{
                if (ri.getTags() == null){
                    ri.setTags(unitItem.getTags());
                }else if (unitItem.getTags() != null){
                    ri.getTags().setInner(unitItem.getTags().getInner());
                }
                Return returnReference = order.getReturns().get(returnId);
                if (!returnReference.isRefunded()) {
                    addToQuote(order.getQuote(), ri);
                    order.getItems().add(ri);
                }
            });
            for (RefundLineItem refundedLineItem : refundLineItemsMap.get(lineItem.getId())) {
                Refunds refunds = order.getRefunds();
                Refund refund = refunds.get(refundedLineItem.getRefundId());
                if (refund == null ){
                    //Refund is not associated with any return
                    refund = new Refund();
                    refund.setId(refundedLineItem.getRefundId());
                    refund.setFulfillmentId(order.getFulfillment().getId()); //The primary fulfillment is being refunded,
                    refunds.add(refund);
                }
                if (refund.getItems() == null){
                    refund.setItems(new NonUniqueItems());
                }


                Fulfillment refundedFulfillment = order.getFulfillments().get(refund.getFulfillmentId());
                Item refundedUnitItem = createItemFromECommerceLineItem(refundedLineItem.getLineItem(), new UnboundedCache<>() {
                    @Override
                    protected RefundLineItems getValue(String s) {
                        return new RefundLineItems();
                    }
                }, new Returns() , new Refunds( ), refundedFulfillment.getId(), refundedLineItem.getQuantity());

                TagGroups itemTrailAttributes = new TagGroups();
                itemTrailAttributes.add(new TagGroup("type","item"));
                itemTrailAttributes.add(new TagGroup("id",refundedUnitItem.getId()));
                itemTrailAttributes.add(new TagGroup("currency",refundedUnitItem.getPrice().getCurrency()));
                itemTrailAttributes.add(new TagGroup("value",-1 * refundedUnitItem.getPrice().getValue() * refundedUnitItem.getQuantity().getCount()));
                refundedFulfillment.getTags().add(new TagGroup("quote_trail",itemTrailAttributes));

                if (refundedUnitItem.getTax() != null && refundedUnitItem.getTax().getValue() > 0) {
                    TagGroups taxTrailAttributes = new TagGroups();
                    taxTrailAttributes.add(new TagGroup("type","tax"));
                    taxTrailAttributes.add(new TagGroup("id",refundedUnitItem.getId()));
                    taxTrailAttributes.add(new TagGroup("currency",refundedUnitItem.getPrice().getCurrency()));
                    taxTrailAttributes.add(new TagGroup("value",-1 * refundedUnitItem.getTax().getValue() * refundedUnitItem.getQuantity().getCount()));
                    refundedFulfillment.getTags().add(new TagGroup("quote_trail",taxTrailAttributes));
                }

                order.getFulfillments().add(refundedFulfillment,true);
                order.getItems().add(refundedUnitItem);
                if (refund.getItems().get(itemId) == null){
                    refund.getItems().add(refundedUnitItem);
                }

            }
            //addToQuote(quote,item); No need to add to quote
        });

        Quote quote = order.getQuote();
        quote.getPrice().setCurrency(order.getPayment().getParams().getCurrency());

        Bucket total = new Bucket();
        for (BreakUpElement element1 : quote.getBreakUp()) {
            total.increment(element1.getPrice().getValue() );
        }

        quote.getPrice().setValue(total.doubleValue());
        /* Recompute quote based on returned products. */


        ShopifyOrder.Address shipping = eCommerceOrder.getShippingAddress();
        if (shipping == null || ObjectUtil.isVoid(shipping.getAddress1())){
            shipping = eCommerceOrder.getBillingAddress();
        }

        Locations locations = getProviderLocations();
        Location providerLocation = locations.get(BecknIdHelper.getBecknId(StringUtil.valueOf(eCommerceOrder.getLocationId()),getSubscriber(),Entity.provider_location));
        order.setProviderLocation(providerLocation);

        fulfillment.setFulfillmentStatus(eCommerceOrder.getFulfillmentStatus());

        if (fulfillment.getStart() == null) {
            fulfillment.setStart(new FulfillmentStop());
            fulfillment.getStart().setLocation(providerLocation);
            fulfillment.getStart().setContact(getProviderConfig().getSupportContact());
        }

        fulfillment.setProviderId(String.format("%s/logistics",getSubscriber().getSubscriberId()));
        fulfillment.setProviderName(getProviderConfig().getFulfillmentProviderName());


        if (fulfillment.getEnd() == null) {
            fulfillment.setEnd(new FulfillmentStop());
            if (lastReturnedOrderJson .getFulfillment() != null && lastReturnedOrderJson.getFulfillment().getEnd() != null) {
                fulfillment.getEnd().update(lastReturnedOrderJson.getFulfillment().getEnd());
            }
        }
        if (fulfillment.getEnd().getLocation() == null) {
            fulfillment.getEnd().setLocation(new Location());
        }
        if (fulfillment.getEnd().getLocation().getAddress() == null) {
            fulfillment.getEnd().getLocation().setAddress(shipping.getAddress());
        }
        if (fulfillment.getEnd().getLocation().getGps() == null) {
            fulfillment.getEnd().getLocation().setGps(new GeoCoordinate(shipping.getLatitude(), shipping.getLongitude()));
        }
        if (fulfillment.getEnd().getContact() == null) {
            fulfillment.getEnd().setContact(new Contact());
            fulfillment.getEnd().getContact().setPhone(shipping.getPhone());
            fulfillment.getEnd().getContact().setEmail(eCommerceOrder.getEmail());
        }
        if (fulfillment.getEnd().getPerson() == null){
            fulfillment.getEnd().setPerson(new Person());
            fulfillment.getEnd().getPerson().setName(fulfillment.getEnd().getLocation().getAddress().getName());
        }
        planFulfillment(order);

        if (auditMap.containsKey(FulfillmentStatus.Order_picked_up)){
            fulfillment.getStart().getTime().setTimestamp(auditMap.get(FulfillmentStatus.Order_picked_up));
        }

        if (auditMap.containsKey(FulfillmentStatus.Order_delivered)){
            fulfillment.getEnd().getTime().setTimestamp(auditMap.get(FulfillmentStatus.Order_delivered));
        }


        fulfillment.setContact(getProviderConfig().getSupportContact());


        Address address = fulfillment.getEnd().getLocation().getAddress();
        fulfillment.setCustomer(new User());
        fulfillment.getCustomer().setPerson(new Person());
        fulfillment.getCustomer().getPerson().setName(address.getName());

        order.getFulfillments().add(fulfillment,true);

        order.setProvider(new Provider());
        order.getProvider().setId(getSubscriber().getSubscriberId());
        order.getProvider().setDescriptor(new Descriptor());
        order.getProvider().getDescriptor().setName(getProviderConfig().getStoreName());
        order.getProvider().setCategoryId(getProviderConfig().getCategory().getId());
        order.getProvider().setLocations(new Locations());
        order.getProvider().getLocations().add(order.getProviderLocation());

        if (lastReturnedOrderJson.getCreatedAt() == null) {
            order.setCreatedAt(eCommerceOrder.getCreatedAt());
        }


        order.setBppTaxNumber(getProviderConfig().getGstIn());

        Cancellation cancellation = lastReturnedOrderJson.getCancellation();
        Option selectedReason = cancellation == null ? null : cancellation.getSelectedReason();
        Descriptor descriptor = selectedReason == null ? null : selectedReason.getDescriptor();
        if (cancellation != null) {
            //buyer cancellation
            cancellation.setId( descriptor == null ? null : descriptor.getCode());
            order.setState(Status.Cancelled);
        }

        if (order.getState() == Status.Cancelled){
            if (order.getCancellation() == null) {
                order.setCancellation(new Cancellation());
                order.getCancellation().setCancelledBy(CancelledBy.PROVIDER);
                order.getCancellation().setSelectedReason(new Option());
                order.getCancellation().getSelectedReason().setDescriptor(new Descriptor());
                descriptor = order.getCancellation().getSelectedReason().getDescriptor();

                switch (eCommerceOrder.getCancelReason()) {
                    case "inventory":
                        descriptor.setCode(CancellationReasonCode.convertor.toString(CancellationReasonCode.ITEM_NOT_AVAILABLE));
                        descriptor.setLongDesc("One or more items in the Order not available");
                        order.getCancellation().setId(descriptor.getCode());
                        break;
                    case "declined":
                        descriptor.setCode(CancellationReasonCode.convertor.toString(CancellationReasonCode.BUYER_REFUSES_DELIVERY));
                        order.getCancellation().setId(descriptor.getCode());
                        break;
                    case "other":
                        descriptor.setCode(CancellationReasonCode.convertor.toString(CancellationReasonCode.REJECT_ORDER));
                        descriptor.setLongDesc("Unable to fulfill order!");
                        order.getCancellation().setId(descriptor.getCode());
                        break;
                }
            }
        }


        return order;
    }

    private void loadInvoice(Order order, ShopifyOrder eCommerceOrder) {
        if (!ObjectUtil.isVoid(eCommerceOrder.getInvoiceUrl())) {
            order.setDocuments(new Documents());
            Document invoice = new Document();
            order.getDocuments().add(invoice);
            invoice.setLabel("Invoice");
            invoice.setUrl(eCommerceOrder.getInvoiceUrl());
        }
    }

    private void loadShippingQuoteBreakup(ShopifyOrder eCommerceOrder, Order order, Bucket orderRefundAmount) {
        Quote quote = new Quote();
        order.setQuote(quote);
        quote.setTtl(15L*60L);
        quote.setPrice(new Price());
        quote.setBreakUp(new BreakUp());

        Fulfillment fulfillment = order.getFulfillment();
        Price shippingPrice = new Price();shippingPrice.setCurrency("INR"); shippingPrice.setValue(0.0);
        Price shippingTaxes = new Price();shippingTaxes.setCurrency("INR");shippingTaxes.setValue(0.0);
        if (eCommerceOrder.getShippingLine() != null) {
            shippingPrice.setValue(eCommerceOrder.getShippingLine().getPrice());
            TaxLines taxLines = eCommerceOrder.getShippingLine().getTaxLines();
            for (TaxLine taxLine : taxLines) {
                shippingTaxes.setValue(shippingTaxes.getValue() + taxLine.getPrice());
            }
        }
        if (shippingPrice.getValue() > 0) {
            BreakUpElement shippingPriceElement = quote.getBreakUp().createElement(BreakUpCategory.delivery, "Delivery Charges", shippingPrice);
            shippingPriceElement.setItemId(fulfillment.getId());

            BreakUpElement shippingTaxesElement = quote.getBreakUp().createElement(BreakUpCategory.delivery, "Delivery Taxes", shippingTaxes);
            shippingTaxesElement.setItemId(fulfillment.getId());


            if (order.getState() != Status.Cancelled) {
                // This is also probably to be checked.
                // localOrderSynchronizer.hasOrderReached(transactionId,Status.Out_for_delivery) ||
                // localOrderSynchronizer.hasOrderReached(transactionId,Status.Completed)
                quote.getBreakUp().add(shippingPriceElement);
                if (!isTaxIncludedInPrice()) {
                    quote.getBreakUp().add(shippingTaxesElement);
                }
            }else {
                initializeRefundedFulfillment(order,eCommerceOrder,fulfillment); //Refund id etc will be assigned later.
                fulfillment.setFulfillmentStatus(FulfillmentStatus.Cancelled);
                TagGroups itemTrailAttributes = new TagGroups();
                itemTrailAttributes.add(new TagGroup("type","Delivery Charges"));
                itemTrailAttributes.add(new TagGroup("id",shippingPriceElement.getItemId()));
                itemTrailAttributes.add(new TagGroup("currency",shippingPriceElement.getPrice().getCurrency()));
                itemTrailAttributes.add(new TagGroup("value",-1 * shippingPriceElement.getPrice().getValue()));
                fulfillment.getTags().add(new TagGroup("quote_trail",itemTrailAttributes));
                orderRefundAmount.increment(shippingPriceElement.getPrice().getValue());

                if (shippingTaxes.getValue() > 0 && !isTaxIncludedInPrice()) {
                    TagGroups taxTrailAttributes = new TagGroups();
                    taxTrailAttributes.add(new TagGroup("type", "Delivery Taxes"));
                    taxTrailAttributes.add(new TagGroup("id", shippingTaxesElement.getItemId()));
                    taxTrailAttributes.add(new TagGroup("currency", shippingTaxesElement.getPrice().getCurrency()));
                    taxTrailAttributes.add(new TagGroup("value", -1 * shippingTaxesElement.getPrice().getValue()));
                    fulfillment.getTags().add(new TagGroup("quote_trail", taxTrailAttributes));
                    orderRefundAmount.increment(shippingTaxesElement.getPrice().getValue());
                }

            }
        }

    }

    private void initializeRefundFulfillments(Order order , ShopifyOrder eCommerceOrder) {
        Refunds refunds = order.getRefunds();
        if (refunds == null){
            refunds = new Refunds();
            order.setRefunds(refunds);
        }

        for (Refund refund :refunds){
            Fulfillment refundedFulfillment = order.getFulfillments().get(refund.getFulfillmentId());
            initializeRefundedFulfillment(order,eCommerceOrder,refundedFulfillment);
        }

    }

    private void initializeRefundedFulfillment(Order order, ShopifyOrder shopifyOrder , Fulfillment refundedFulfillment) {
        TagGroups tagGroups = refundedFulfillment.getTags();
        if (tagGroups == null){
            tagGroups = new TagGroups();
            refundedFulfillment.setTags(tagGroups);
        }
        for (int i = 0 ; i < tagGroups.size() ; i ++ ){
            TagGroup tg = tagGroups.get(i);
            if (tg.getId().equals("quote_trail")){
                tagGroups.remove(i);
                i --;
            }
        }
        if (refundedFulfillment.getType() != FulfillmentType.return_to_origin) {
            if (tagGroups.getTag("cancel_request","reason_id") == null){
                String cancellationCode =  CancellationReasonCode.convertor.toString(CancellationReasonCode.ITEM_NOT_AVAILABLE);
                if (order.getCancellation() != null){
                    cancellationCode =  order.getCancellation().getSelectedReason().getDescriptor().getCode();
                }
                tagGroups.setTag("cancel_request", "reason_id", cancellationCode);
            }
            if (tagGroups.getTag("cancel_request","initiated_by") == null){
                String initiatedBy = getSubscriber().getSubscriberId();
                if (order.getCancellation() != null){
                    Context context = shopifyOrder.getContext();
                    initiatedBy = (order.getCancellation().getCancelledBy() == CancelledBy.BUYER ? context.getBapId() : context.getBppId());
                }
                tagGroups.setTag("cancel_request","initiated_by",initiatedBy);
            }
        }

    }

    private void planFulfillment(Order order) {
        if (order.getCreatedAt() == null){
            return;
        }
        Fulfillment fulfillment  = order.getFulfillment();
        Time startTime = fulfillment.getStart().getTime();
        if (startTime == null){
            startTime = new Time();
            Range range = new Range();
            startTime.setRange(range);
            range.setStart(order.getCreatedAt());
            range.setEnd(DateUtils.addHours(order.getCreatedAt(),(int)getProviderConfig().getTimeToShip().toHours()));
            fulfillment.getStart().setTime(startTime);
        }
        Time endTime = fulfillment.getEnd().getTime();

        if (endTime == null){
            endTime = new Time();
            Range range = new Range();
            endTime.setRange(range);
            range.setStart(order.getCreatedAt());
            range.setEnd(DateUtils.addHours(order.getCreatedAt(),(int)getProviderConfig().getFulfillmentTurnAroundTime().toHours()));
            fulfillment.getEnd().setTime(endTime);
        }
    }

    private Map<String, Map<String, Item>> getReturnedItems(Order order) {
        Map<String,Map<String,Item>> returnedItems  = new UnboundedCache<>() {
            @Override
            protected Map<String, Item> getValue(String itemId) {
                return new UnboundedCache<>() {
                    @Override
                    protected Item getValue(String returnId) {
                        return null;
                    }
                };
            }
        };
        if (order.getReturns() == null){
            order.setReturns(new Returns());
        }
        for (Return r : order.getReturns()) {
            Fulfillment returnFulfillment = order.getFulfillments().get(r.getFulfillmentId());
            switch (r.getReturnStatus()){
                case REQUESTED:
                    returnFulfillment.setFulfillmentStatus(FulfillmentStatus.Return_Initiated);
                    break;
                case OPEN:
                    returnFulfillment.setFulfillmentStatus(FulfillmentStatus.Return_Approved);
                    break;
                case DECLINED:
                case CANCELED:
                    returnFulfillment.setFulfillmentStatus(FulfillmentStatus.Return_Rejected);
                    if (r.getReturnRejectReason() != null ) {
                        returnFulfillment.getState().getDescriptor().setShortDesc(
                                ReturnRejectReason.convertor.toString(r.getReturnRejectReason()));
                    }
                    break;
                case REFUNDED:
                case CLOSED:
                    returnFulfillment.setFulfillmentStatus(FulfillmentStatus.Return_Delivered);
                    break;
            }

            for (Item ri : r.getItems()) {
                if (ri.getTags() == null){
                    ri.setTags(new TagGroups());
                }
                ri.setFulfillmentId(r.getFulfillmentId()); //This is redundant. but ok
                returnedItems.get(ri.getId()).put(r.getId(),ri);
            }
        }
        return returnedItems;
    }

    private void addToQuote(Quote quote, Item unitItem) {
        if (unitItem.getQuantity().getCount() > 0){
            BreakUpElement breakUpElement = quote.getBreakUp().createElement(BreakUpCategory.item,unitItem.getDescriptor().getName(),unitItem.getPrice(),unitItem.getQuantity().getCount()); //quote must have unit price.
            breakUpElement.setItemQuantity(unitItem.getQuantity());
            breakUpElement.setItemId(unitItem.getId());
            quote.getBreakUp().add(breakUpElement);

            if (!isTaxIncludedInPrice()) {
                BreakUpElement element = quote.getBreakUp().createElement(BreakUpCategory.tax,"Tax",unitItem.getTax(),unitItem.getQuantity().getCount());
                element.setItemId(unitItem.getId());

                quote.getBreakUp().add(element);
            }

            Item quoteItem = new Item((JSONObject) JSONAwareWrapper.parse(unitItem.toString()));
            quoteItem.setFulfillmentId(null);
            quoteItem.setQuantity(null);
            breakUpElement.setItem(quoteItem);
        }
    }
    private Item createItemFromECommerceLineItem(LineItem eCommerceLineItem, Map<String, RefundLineItems> refundedMap, Returns returns, Refunds refunds, String fulfillmentId){
        return createItemFromECommerceLineItem(eCommerceLineItem,refundedMap,returns,  refunds,fulfillmentId,doubleTypeConverter.valueOf(eCommerceLineItem.getQuantity()).intValue());
    }
    private Item createItemFromECommerceLineItem(LineItem eCommerceLineItem, Map<String, RefundLineItems> refundedMap, Returns returns, Refunds refunds, String fulfillmentId, int overrideLineQuantity) {
        Item unitItem = new Item();
        in.succinct.bpp.search.db.model.Item dbItem = getItem(BecknIdHelper.getBecknId(String.valueOf(eCommerceLineItem.getVariantId()), getSubscriber(), Entity.item));
        if (dbItem == null){
            throw new SellerException.ItemNotFound();
        }
        Item itemIndexed  = new Item((JSONObject) JSONAwareWrapper.parse(dbItem.getObjectJson()));
        unitItem.update(itemIndexed);
        Quantity quantity = new Quantity();

        int refundQty = 0;
        for (RefundLineItem lineItem : refundedMap.get(eCommerceLineItem.getId())) {
            refundQty += lineItem.getQuantity();
        }

        int returnProcessingQty = 0 ;
        for (Return r  : returns){
            Item returnedItem = r.getItems().get(unitItem.getId());
            if(returnedItem != null &&  !r.isRefunded()){
                returnProcessingQty += returnedItem.getQuantity().getCount();
            }
        }



        quantity.setCount(overrideLineQuantity - refundQty - returnProcessingQty);

        unitItem.setQuantity(quantity);
        unitItem.setFulfillmentId(fulfillmentId);
        unitItem.setFulfillmentIds(null);
        unitItem.setTags(null);
        return unitItem;

    }




    private void setPayment(Payment payment, ShopifyOrder eCommerceOrder , double refundAmount) {

        if (payment.getCollectedBy() == null){
            if (getProviderConfig().isCodSupported()){
                payment.setCollectedBy(CollectedBy.BPP);
                payment.setType(PaymentType.POST_FULFILLMENT);
            }else {
                payment.setCollectedBy(CollectedBy.BAP);
                payment.setType(PaymentType.ON_ORDER);
            }
        }else if (payment.getCollectedBy() == CollectedBy.BPP  && getProviderConfig().isCodSupported()){
            payment.setType(PaymentType.POST_FULFILLMENT);
        }else{
            payment.setCollectedBy(CollectedBy.BAP);
            payment.setType(PaymentType.ON_ORDER);
        }
        // Part of RSP Protocol!
        payment.setCollectedByStatus(NegotiationStatus.Agree);
        payment.setSettlementBasis(SettlementBasis.collection);
        payment.setSettlementBasisStatus(NegotiationStatus.Agree);
        payment.setReturnWindow(getProviderConfig().getReturnWindow());
        payment.setReturnWindowStatus(NegotiationStatus.Assert);

        if (payment.getParams() == null) {
            payment.setParams(new Params());
            payment.getParams().setCurrency(getStore().getCurrency());
        }
        if (eCommerceOrder.getPaymentTerms() != null) {
            payment.getParams().setAmount(doubleTypeConverter.valueOf(eCommerceOrder.getPaymentTerms().getAmount()) - refundAmount);
        }else {
            payment.getParams().setAmount(eCommerceOrder.getTotalPrice() - refundAmount);
        }


        SettlementDetail detail = null;
        if (payment.getSettlementDetails() == null){
            payment.setSettlementDetails( new SettlementDetails());
            detail = new SettlementDetail();
            detail.setPayload(getProviderConfig().getSettlementDetail().toString());
            payment.getSettlementDetails().add(detail);
        }else if (payment.getSettlementDetails().size() == 1){
            for (SettlementDetail d : payment.getSettlementDetails()) {
                if (d.getSettlementStatus() == PaymentStatus.NOT_PAID &&
                        d.getSettlementPhase() == SettlementPhase.SALE_AMOUNT) {
                    detail = d;
                    break;
                }
            }
        }
        if (detail != null) {
            detail.setSettlementType(SettlementType.UPI);
            detail.setSettlementPhase(SettlementPhase.SALE_AMOUNT);
            detail.setSettlementCounterparty(SettlementCounterparty.SELLER_APP);
            detail.setSettlementStatus(PaymentStatus.NOT_PAID);
            boolean paid = eCommerceOrder.isPaid();
            boolean isSettled = eCommerceOrder.isSettled();

            if (payment.getCollectedBy() == CollectedBy.BPP) {
                if (paid) {
                    payment.setStatus(PaymentStatus.PAID);
                    detail.setSettlementStatus(isSettled ? PaymentStatus.PAID : PaymentStatus.NOT_PAID);
                } else {
                    payment.setStatus(PaymentStatus.NOT_PAID);
                    detail.setSettlementStatus(PaymentStatus.NOT_PAID);
                }
            } else {
                if (paid) {
                    payment.setStatus(PaymentStatus.PAID);
                    detail.setSettlementStatus(isSettled ? PaymentStatus.PAID : PaymentStatus.NOT_PAID);
                }
            }
        }

    }

    private void setBilling(Order target, ShopifyOrder eCommerceOrder) {
        ShopifyOrder.Address source = eCommerceOrder.getBillingAddress();
        if (target.getBilling() != null){
            return;
        }
        Billing billing = new Billing();
        target.setBilling(billing);
        billing.setCreatedAt(eCommerceOrder.getCreatedAt());
        billing.setUpdatedAt(eCommerceOrder.getUpdatedAt());


        if (source.getCountryCode() == null){
            in.succinct.beckn.Country country = getProviderConfig().getLocation().getCountry();
            if (country != null) {
                source.setCountryCode(country.getCode()); //Same Country.
            }else {
                source.setCountryCode(getProviderConfig().getLocation().getAddress().getCountry());
            }
        }


        Address address = source.getAddress();
        billing.setName(address.getName());
        billing.setAddress(address);
        billing.getAddress().setCountry(Country.findByISO(source.getCountryCode()).getIsoCode()); //We need to send code. Shopify returns name
        billing.setPhone(source.getPhone());
        billing.setEmail(eCommerceOrder.getEmail());
    }
    @Override
    public void update(Request request, Request reply) {
        String update_target = request.getMessage().getUpdateTarget();
        if (ObjectUtil.isVoid(update_target) || (!ObjectUtil.equals(update_target, "item") && !ObjectUtil.equals(update_target, "payment"))) {
            throw new SellerException.UpdationNotPossible("\"item\" and \"payment\" are the only update_target supported");
        }
        Order input = request.getMessage().getOrder();
        NonUniqueItems items =  new NonUniqueItems();
        Bucket totalPossibleRefund = new Bucket();
        {
            Fulfillments fulfillments = input.getFulfillments();
            if (fulfillments == null) {
                fulfillments = new Fulfillments();
            }
            for (Fulfillment fulfillment : fulfillments) {
                switch (fulfillment.getType()){
                    case cancel:
                        addCancelRequests(fulfillment, items, request,totalPossibleRefund);
                        break;
                    case return_to_origin:
                        addReturnRequests(fulfillment, items , request,totalPossibleRefund);
                        break;
                }
            }
        }
        Map<String,NonUniqueItems> fulfillmentItemMap = new UnboundedCache<>(){
            @Override
            protected NonUniqueItems getValue(String s) {
                return new NonUniqueItems();
            }
        };
        for (Item item : items){
            fulfillmentItemMap.get(item.getFulfillmentId()).add(item);
        }


        if (ObjectUtil.equals(update_target, "item")) {
            ShopifyOrder localOrder = getShopifyOrder(input);
            Order lastKnownOrder = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLastKnownOrder(request.getContext().getTransactionId(),true);

            JSONObject jsfulfillments = helper.get(String.format("/orders/%s/fulfillments.json", LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(input)), new JSONObject());

            ShopifyOrder.Fulfillments shopifyFulfillments = (new ShopifyOrder.Fulfillments((JSONArray) jsfulfillments.get("fulfillments")));
            localOrder.setFulfillments(shopifyFulfillments);

            Map<String, LineItem> lineItemMap = new HashMap<>();
            if (shopifyFulfillments.isEmpty()) {
                for (LineItem lineItem : localOrder.getLineItems()) {
                    lineItemMap.put(BecknIdHelper.getBecknId(String.valueOf(lineItem.getVariantId()), getSubscriber(), Entity.item), lineItem);
                }
            }else {
                for (ShopifyOrder.Fulfillment fulfillment : shopifyFulfillments){
                    for (LineItem lineItem : fulfillment.getLineItems()){
                        lineItemMap.put(BecknIdHelper.getBecknId(String.valueOf(lineItem.getVariantId()),getSubscriber(),Entity.item),lineItem);
                    }
                }
            }
            for (Fulfillment fulfillment : input.getFulfillments()){
                NonUniqueItems becknItemsReturned = new NonUniqueItems();
                NonUniqueItems becknItemsLiquidated = new NonUniqueItems();

                ShopifyRefund calculateRefundInput = new ShopifyRefund();
                calculateRefundInput.setRefundLineItems(new RefundLineItems());
                StringBuilder refundNotes = new StringBuilder();


                JSONObject returnInput = new JSONObject();
                returnInput.put("orderId", String.format("gid://shopify/Order/%s", localOrder.getId()));
                returnInput.put("returnLineItems", new JSONArray());


                for (Item item : fulfillmentItemMap.get(fulfillment.getId())){
                    TagGroups tagGroups = fulfillment.getTags();
                    for (TagGroup tagGroup : tagGroups){
                        if (tagGroup.getId().equals(fulfillment.getType() == FulfillmentType.return_to_origin ?  "return_request" : "cancel_request")){
                            TagGroups tags = Objects.requireNonNull(tagGroup).getList();
                            String item_id = tags.get("item_id").getValue();
                            if (!item.getId().equals(item_id)){
                                continue;
                            }

                            String item_quantity = tags.get("item_quantity").getValue();
                            String reason_code = tags.get("reason_id").getValue();

                            TagGroup reason_description = tags.get("reason_description");
                            TagGroup tgImages = tags.get("images");
                            TagGroup ttl_approval = tags.get("ttl_approval");
                            TagGroup ttl_reverseqc = tags.get("ttl_reverseqc");

                            int quantity = item.getQuantity().getCount();
                            String image = tgImages == null ? null : tgImages.getValue();

                            String reason_text = reason_description != null ? reason_description.getValue() : null ;
                            if (refundNotes.length() > 0) {
                                refundNotes.append("\n");
                            }
                            item.setDescriptor(lastKnownOrder.getItems().get(item.getId()).getDescriptor());
                            item.setPrice(new Price());
                            item.getPrice().setValue(lastKnownOrder.getItems().get(item.getId()).getPrice().getValue()); //unite price at which the product was sold.
                            item.getPrice().setCurrency(lastKnownOrder.getItems().get(item.getId()).getPrice().getCurrency());

                            if (fulfillment.getType() == FulfillmentType.return_to_origin) {
                                Images images = new Images();
                                String[] imgArray = ObjectUtil.isVoid(image) ? new String[]{} : image.split(",");
                                for (String img : imgArray) {
                                    if (!ObjectUtil.isVoid(img)) {
                                        images.add(img);
                                    }
                                }

                                ReturnReasonCode reasonCode = ReturnReasonCode.convertor.valueOf(reason_code);

                                LineItem lineItem = lineItemMap.get(item.getId());
                                RefundLineItem refundLineItem = new RefundLineItem(lineItem, quantity, reasonCode, getProviderConfig(),totalPossibleRefund);
                                if (!refundLineItem.getRestockType().equals("return")) {
                                    refundNotes.append(String.format("Return Liquidated : %s - Item %s", reasonCode.name(), item.getId()));
                                    becknItemsLiquidated.add(item);
                                    calculateRefundInput.getRefundLineItems().add(refundLineItem);
                                } else {
                                    becknItemsReturned.add(item);
                                    JSONObject returnLineItem = new JSONObject();
                                    returnLineItem.put("quantity", quantity);
                                    returnLineItem.put("fulfillmentLineItemId", String.format("gid://shopify/FulfillmentLineItem/%s", lineItem.getFulfillmentLineItemId()));
                                    String customShopifyReason = getShopifyReturnReason(reasonCode);
                                    String shopifyReasonCode = customShopifyReason.startsWith("Custom:") ? "OTHER" : customShopifyReason;
                                    String shopifyReasonNote = shopifyReasonCode.equals("OTHER") ? customShopifyReason : "";
                                    if (shopifyReasonCode.equals("OTHER") && !ObjectUtil.isVoid(reason_text)) {
                                        shopifyReasonNote += reason_text;
                                    }

                                    returnLineItem.put("returnReason", shopifyReasonCode);
                                    if (!ObjectUtil.isVoid(shopifyReasonNote)) {
                                        returnLineItem.put("customerNote", shopifyReasonNote);
                                    }

                                    ((JSONArray) returnInput.get("returnLineItems")).add(returnLineItem);
                                }
                            } else if (FulfillmentType.cancel == fulfillment.getType()) {

                                becknItemsLiquidated.add(item);
                                CancellationReasonCode reasonCode = CancellationReasonCode.convertor.valueOf(reason_code);
                                refundNotes.append(String.format("Cancellation: %s - Item %s", reasonCode.name(), item.getId()));

                                RefundLineItem refundLineItem = new RefundLineItem(lineItemMap.get(item.getId()), quantity, reasonCode, getProviderConfig(),totalPossibleRefund);
                                calculateRefundInput.getRefundLineItems().add(refundLineItem);
                            } else {
                                throw new SellerException.UpdationNotPossible("Unexpected fulfillment type");
                            }
                        }
                    }
                }
                if (!calculateRefundInput.getRefundLineItems().isEmpty()) {
                    calculateRefundInput.setNote(refundNotes.toString());
                    JSONObject calculateRefundInputHolder = new JSONObject();
                    calculateRefundInputHolder.put("refund", calculateRefundInput);
                    JSONObject calculatedRefundHolder = helper.post(String.format("/orders/%s/refunds/calculate.json", localOrder.getId()), calculateRefundInputHolder);
                    ShopifyRefund calculated = new ShopifyRefund((JSONObject) calculatedRefundHolder.get("refund"));
                    calculated.setNote(calculateRefundInput.getNote()); // Shopify bug! reset
                    calculated.getTransactions().forEach(t -> {
                        if (t.getKind().equals("suggested_refund")) {
                            t.setKind("refund");
                        }
                    });


                    JSONObject refundHolder = helper.post(String.format("/orders/%s/refunds.json", localOrder.getId()), calculatedRefundHolder);
                    ShopifyRefund shopifyRefund = new ShopifyRefund((JSONObject) refundHolder.get("refund"));


                    String fulfillmentId = fulfillment.getId();

                    Refund refund = new Refund();
                    refund.setId(shopifyRefund.getId());
                    refund.setFulfillmentId(fulfillmentId);
                    refund.setCreatedAt(shopifyRefund.getCreatedAt());
                    refund.setItems(becknItemsLiquidated);
                    Refunds refunds = lastKnownOrder.getRefunds();
                    if (refunds == null ){
                        refunds = new Refunds();
                        lastKnownOrder.setRefunds(refunds);
                    }
                    if (fulfillment.getType() == FulfillmentType.return_to_origin) {
                        fulfillment.setFulfillmentStatus(FulfillmentStatus.Return_Liquidated);
                    }else {
                        fulfillment.setFulfillmentStatus(FulfillmentStatus.Cancelled);
                    }
                    lastKnownOrder.getFulfillments().add(fulfillment,true);
                    refunds.add(refund);

                    LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).sync(request.getContext().getTransactionId(),lastKnownOrder);
                }
                if (!((JSONArray) returnInput.get("returnLineItems")).isEmpty()) {
                    // create Return.!!
                    JSONObject payload = new JSONObject();
                    payload.put("query", "mutation returnRequest($input: ReturnRequestInput!) { returnRequest(input: $input) { return { id, status } userErrors { field message } } }");
                    JSONObject variables = new JSONObject();
                    payload.put("variables", variables);
                    variables.put("input", returnInput);
                    JSONObject out = helper.post("graphql.json", payload);
                    JSONObject r = (JSONObject) ((JSONObject) (((JSONObject) out.get("data")).get("returnRequest"))).get("return");
                    Return returnReference = new Return();
                    returnReference.setReturnMessageId(request.getContext().getMessageId());
                    returnReference.setId((String) r.get("id"));
                    returnReference.setReturnStatus(ReturnStatus.convertor.valueOf((String) r.get("status")));
                    returnReference.setItems(becknItemsReturned);
                    returnReference.setCreatedAt(new Date());
                    returnReference.setFulfillmentId(fulfillment.getId());
                    if (lastKnownOrder.getReturns() == null){
                        lastKnownOrder.setReturns(new Returns());
                    }
                    lastKnownOrder.getReturns().add(returnReference);
                    LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).sync(request.getContext().getTransactionId(),lastKnownOrder);
                }
            }
        }

        reply.setMessage(new Message());
        Order current = getStatus(input);
        if (ObjectUtil.equals(update_target, "payment")) {
            Fulfillments fulfillments = input.getFulfillments();
            Fulfillment fulfillment = current.getFulfillments().get(fulfillments.get(0).getId());
            if (fulfillment == null){
                throw new UpdationNotPossible("Invalid fulfillment");
            }
            if (fulfillment.getSettlementDetails() == null){
                fulfillment.setSettlementDetails(new SettlementDetails());
            }


            SettlementDetails newDetails = input.getPayment().getSettlementDetails();
            SettlementDetails settlementDetails = current.getPayment().getSettlementDetails();
            List<SettlementDetail> toAdd = new ArrayList<>();
            for (SettlementDetail newDetail : newDetails) {
                boolean found = false;
                for (SettlementDetail oldDetail : settlementDetails) {
                    found = found || oldDetail.equals(newDetail);
                }
                if (!found) {
                    toAdd.add(newDetail);
                }
            }
            for (SettlementDetail detail : toAdd) {
                settlementDetails.add(detail);
                fulfillment.getSettlementDetails().add(detail);
            }


        }
        reply.getMessage().setOrder(current);



    }

    private void addCancelRequests(Fulfillment fulfillment, NonUniqueItems items, Request request, Bucket totalPossibleRefund) {
        if (!ObjectUtil.equals(fulfillment.getType(),FulfillmentType.cancel)) {
            return ;
        }
        Order lastKnownOrder = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLastKnownOrder(request.getContext().getTransactionId(),true);

        TagGroups tagGroups = fulfillment.getTags();
        for (TagGroup tagGroup : tagGroups.all("cancel_request")){
            TagGroups tags = tagGroup.getList();
            String fulfillmentId = fulfillment.getId();
            if (fulfillmentId == null){
                TagGroup g  = tags.get("id");
                if (g != null){
                    fulfillmentId = g.getValue();
                }
            }
            if (fulfillmentId == null){
                fulfillmentId = String.format("%s/%s",FulfillmentType.cancel,request.getContext().getMessageId());
            }
            if (fulfillment.getId() == null) {
                fulfillment.setId(fulfillmentId);
            }
            String item_id = tags.get("item_id").getValue();
            String item_quantity = tags.get("item_quantity").getValue();
            String reason_id = tags.get("reason_id").getValue();
            TagGroup initatedBy = tags.get("initiated_by");
            if (initatedBy == null){
                initatedBy = new TagGroup();
                initatedBy.setId("initiated_by");
                initatedBy.setValue(request.getContext().getBapId());
                tags.add(initatedBy);
            }

            Item item = new Item();
            item.setId(item_id);
            Quantity q = new Quantity(); q.setCount(item_quantity);
            item.setQuantity(q);
            item.setFulfillmentId(fulfillmentId);
            Double unitPrice = lastKnownOrder.getItems().get(item_id).getPrice().getValue();
            if (unitPrice != null){
                totalPossibleRefund.increment(unitPrice * q.getCount());
            }
            items.add(item);

        }
        fulfillment.setContact(getProviderConfig().getSupportContact());
        fulfillment.setProviderName(getProviderConfig().getFulfillmentProviderName());
        fulfillment.setTracking(false);
        fulfillment.setCategory(getProviderConfig().getFulfillmentCategory().toString());
        if (fulfillment.getTAT() == null) {
            fulfillment.setTAT(getProviderConfig().getFulfillmentTurnAroundTime());
        }

    }

    private void addReturnRequests(Fulfillment fulfillment, NonUniqueItems items, Request request, Bucket totalPossibleRefund) {
        if (!ObjectUtil.equals(fulfillment.getType(),FulfillmentType.return_to_origin)) {
            return ;
        }
        Order lastKnownOrder = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLastKnownOrder(request.getContext().getTransactionId(),true);
        TagGroup ttl_reverseqc = new TagGroup();
        ttl_reverseqc.setId("ttl_reverseqc");
        ttl_reverseqc.setValue(getProviderConfig().getFulfillmentTurnAroundTime().toString());


        TagGroups tagGroups = fulfillment.getTags();
        for (TagGroup tagGroup : tagGroups.all("return_request") ){
            TagGroups tags = tagGroup.getList();

            String fulfillmentId = fulfillment.getId();
            if (fulfillmentId == null){
                TagGroup g  = tags.get("id");
                if (g != null){
                    fulfillmentId = g.getValue();
                }
            }
            if (fulfillmentId == null){
                fulfillmentId = String.format("%s/%s",FulfillmentType.return_to_origin,request.getContext().getMessageId());
            }
            if (fulfillment.getId() == null) {
                fulfillment.setId(fulfillmentId);
            }

            String item_id = tags.get("item_id").getValue();
            String item_quantity = tags.get("item_quantity").getValue();
            String reason_id = tags.get("reason_id").getValue();

            TagGroup reason_description = tags.get("reason_description");
            TagGroup images = tags.get("images");
            TagGroup ttl_approval = tags.get("ttl_approval");
            TagGroup item_ttl_reverse_qc = tags.get("ttl_reverseqc");

            if (item_ttl_reverse_qc != null){
                Duration d_ttl_reverse_qc = Duration.parse(ttl_reverseqc.getValue());
                Duration d_item_ttl_reverse_qc =  Duration.parse(item_ttl_reverse_qc.getValue());
                if (d_item_ttl_reverse_qc.compareTo(d_ttl_reverse_qc) < 0){
                    d_ttl_reverse_qc =d_item_ttl_reverse_qc;
                }
                ttl_reverseqc.setValue(d_ttl_reverse_qc.toString());
            }else {
                tags.set("ttl_reverseqc", ttl_reverseqc);
            }

            TagGroup initatedBy = tags.get("initiated_by");
            if (initatedBy == null){
                initatedBy = new TagGroup();
                initatedBy.setId("initiated_by");
                initatedBy.setValue(request.getContext().getBapId());
                tags.add(initatedBy);
            }


            Item item = new Item();
            item.setId(item_id);
            Quantity q = new Quantity();q.setCount(item_quantity);
            item.setQuantity(q);
            item.setFulfillmentId(fulfillmentId);
            Double unitPrice = lastKnownOrder.getItems().get(item_id).getPrice().getValue();
            if (unitPrice != null){
                totalPossibleRefund.increment(unitPrice * q.getCount());
            }
            items.add(item);
        }

        fulfillment.setContact(getProviderConfig().getSupportContact());
        fulfillment.setProviderName(getProviderConfig().getFulfillmentProviderName());
        fulfillment.setTracking(false);
        fulfillment.setCategory(getProviderConfig().getFulfillmentCategory().toString());
        fulfillment.setTAT(getProviderConfig().getFulfillmentTurnAroundTime());
        fulfillment.setStart(new FulfillmentStop(JSONAwareWrapper.parse(lastKnownOrder.getFulfillment().getEnd().toString())));

        fulfillment.getStart().setTime(new Time());
        fulfillment.getStart().getTime().setRange(new Range());

        fulfillment.getStart().getTime().getRange().setStart(request.getContext().getTimestamp());
        fulfillment.getStart().getTime().getRange().setEnd(DateUtils.addHours(fulfillment.getStart().getTime().getRange().getStart(),(int)Duration.parse(ttl_reverseqc.getValue()).toHours()));


        fulfillment.setEnd(new FulfillmentStop(JSONAwareWrapper.parse(lastKnownOrder.getFulfillment().getStart().toString())));
        fulfillment.getEnd().setTime(new Time());
        fulfillment.getEnd().getTime().setRange(new Range());
        fulfillment.getEnd().getTime().getRange().setStart(fulfillment.getStart().getTime().getRange().getStart());
        fulfillment.getEnd().getTime().getRange().setEnd(DateUtils.addHours(fulfillment.getEnd().getTime().getRange().getStart(),(int)getProviderConfig().getFulfillmentTurnAroundTime().toHours()));
    }


    public String getShopifyReturnReason(ReturnReasonCode returnReasonCode){
        switch (returnReasonCode) {
            case ITEM_DAMAGED:
                return "DEFECTIVE";
            case ITEM_MISMATCH:
                return "WRONG_ITEM";
            case ITEM_QUANTITY_MISMATCH:
                return "Custom: Qty mismatch!";
            case ITEM_NOT_REQUIRED:
            case ITEM_PRICE_TOO_HIGH:
            default:
                return "UNWANTED";
        }
    }



}
