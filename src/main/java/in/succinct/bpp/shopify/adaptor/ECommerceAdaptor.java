package in.succinct.bpp.shopify.adaptor;

import com.venky.cache.Cache;
import com.venky.core.math.DoubleHolder;
import com.venky.core.math.DoubleUtils;
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
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Billing;
import in.succinct.beckn.BreakUp;
import in.succinct.beckn.BreakUp.BreakUpElement;
import in.succinct.beckn.BreakUp.BreakUpElement.BreakUpCategory;
import in.succinct.beckn.Contact;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Document;
import in.succinct.beckn.Documents;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.Fulfillment.FulfillmentType;
import in.succinct.beckn.FulfillmentStop;
import in.succinct.beckn.Images;
import in.succinct.beckn.Item;
import in.succinct.beckn.Item.PackagedCommodity;
import in.succinct.beckn.Item.PrepackagedFood;
import in.succinct.beckn.Item.VeggiesFruits;
import in.succinct.beckn.ItemQuantity;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.Status;
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payment.CollectedBy;
import in.succinct.beckn.Payment.CommissionType;
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
import in.succinct.beckn.SellerException.GenericBusinessError;
import in.succinct.beckn.SellerException.OrderConfirmFailure;
import in.succinct.beckn.SettlementDetail;
import in.succinct.beckn.SettlementDetail.SettlementCounterparty;
import in.succinct.beckn.SettlementDetail.SettlementPhase;
import in.succinct.beckn.SettlementDetail.SettlementType;
import in.succinct.beckn.SettlementDetails;
import in.succinct.beckn.Tag;
import in.succinct.beckn.Tags;
import in.succinct.beckn.User;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.FulfillmentStatusAdaptor;
import in.succinct.bpp.core.adaptor.FulfillmentStatusAdaptor.FulfillmentStatusAudit;
import in.succinct.bpp.core.adaptor.TimeSensitiveCache;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper.Entity;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.ProviderConfig.Serviceability;
import in.succinct.bpp.shopify.adaptor.ECommerceSDK.Page;
import in.succinct.bpp.shopify.model.ProductImages;
import in.succinct.bpp.shopify.model.ProductImages.ProductImage;
import in.succinct.bpp.shopify.model.Products;
import in.succinct.bpp.shopify.model.Products.InventoryItem;
import in.succinct.bpp.shopify.model.Products.InventoryItems;
import in.succinct.bpp.shopify.model.Products.InventoryLevel;
import in.succinct.bpp.shopify.model.Products.InventoryLevels;
import in.succinct.bpp.shopify.model.Products.Product;
import in.succinct.bpp.shopify.model.Products.ProductVariant;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import in.succinct.bpp.shopify.model.ShopifyOrder.Fulfillments;
import in.succinct.bpp.shopify.model.ShopifyOrder.LineItem;
import in.succinct.bpp.shopify.model.ShopifyOrder.LineItems;
import in.succinct.bpp.shopify.model.ShopifyOrder.NoteAttributes;
import in.succinct.bpp.shopify.model.ShopifyOrder.PaymentSchedule;
import in.succinct.bpp.shopify.model.ShopifyOrder.PaymentSchedules;
import in.succinct.bpp.shopify.model.ShopifyOrder.PaymentTerms;
import in.succinct.bpp.shopify.model.ShopifyOrder.ShippingLine;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transaction;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transactions;
import in.succinct.bpp.shopify.model.Store;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class ECommerceAdaptor extends CommerceAdaptor {
    final ECommerceSDK helper;
    final TimeSensitiveCache cache = new TimeSensitiveCache(Duration.ofDays(1));

    public ECommerceAdaptor(Map<String, String> configuration, Subscriber subscriber) {
        super(configuration, subscriber);
        this.helper = new ECommerceSDK(this);
        getProviderConfig().getSupportContact().setEmail(getSupportEmail());
        getProviderConfig().setLocation(getProviderLocations().get(0));
    }


    public String getBecknTransactionId(ShopifyOrder draftOrder){
       for (Tag noteAttribute : draftOrder.getNoteAttributes()) {
            if (noteAttribute.getName().equals("context.transaction_id")){
                return noteAttribute.getValue();
            }
        }
        return null;
    }


    public Order initializeDraftOrder(Request request) {
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

        shopifyOrder.setId(LocalOrderSynchronizer.getInstance().getLocalOrderId(request.getContext().getTransactionId()));

        shopifyOrder.setCurrency("INR");
        shopifyOrder.setSourceName("beckn");
        shopifyOrder.setName("beckn-" + request.getContext().getTransactionId());
        shopifyOrder.setNoteAttributes(new NoteAttributes());

        for (String key : new String[]{"bap_id", "bap_uri", "domain", "transaction_id", "city", "country", "core_version"}) {
            Tag meta = new Tag();
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

        setBilling( bo.getBilling(),shopifyOrder);
        Bucket totalPrice = new Bucket();
        shopifyOrder.setLocationId(Long.parseLong(BecknIdHelper.getLocalUniqueId(getProviderConfig().getLocation().getId(),Entity.provider_location)));

        if (serviceability != null) {
            ShippingLine shippingLine = new ShippingLine();
            shippingLine.setTitle("Standard");
            shippingLine.setPrice(serviceability.getCharges());
            shippingLine.setCode("Local Delivery");
            shippingLine.setCustom(false);
            shippingLine.setPhone(shopifyOrder.getShippingAddress().getPhone());
            shippingLine.setSource("shopify");
            shopifyOrder.setShippingLine(shippingLine);
            totalPrice.increment(serviceability.getCharges());
        }


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
                LineItem lineItem = addItem(shopifyOrder, refreshedBoItem);
                totalPrice.increment(refreshedBoItem.getPrice().getValue() * lineItem.getQuantity());
            });
        }

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

        return saveDraftOrder(shopifyOrder);
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
        lineItem.setTaxable(doubleTypeConverter.valueOf(item.getTags().get("tax_rate")) > 0);
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

    @SuppressWarnings("unchecked")
    private Order saveDraftOrder(ShopifyOrder draftOrder) {
        JSONObject dro = new JSONObject();
        dro.put("order",draftOrder.getInner());

        JSONObject outOrder = helper.post("/orders.json", dro);
        ShopifyOrder oDraftOrder = new ShopifyOrder((JSONObject) outOrder.get("order"));
        return getBecknOrder(oDraftOrder);

    }

    @Override
    public Order confirmDraftOrder(Order inOrder) {
        String shopifyOrderId = LocalOrderSynchronizer.getInstance().getLocalOrderId(inOrder);
        JSONObject response = helper.get(String.format("/orders/%s.json", shopifyOrderId), new JSONObject());
        ShopifyOrder shopifyOrder = new ShopifyOrder((JSONObject) response.get("order"));

        if (Config.instance().isDevelopmentEnvironment() && inOrder.getPayment().getStatus() == PaymentStatus.PAID) {
            JSONObject transactionsJs = helper.get(String.format("/orders/%s/transactions.json", shopifyOrderId), new JSONObject());
            Transactions transactions = new Transactions((JSONArray) transactionsJs.get("transactions"));

            Transaction transaction = transactions.get(0);
            transaction.setParentId(Long.parseLong(transaction.getId()));
            transaction.setKind("capture");
            transaction.rm("id");
            JSONObject params = new JSONObject(); params.put("transaction",transaction.getInner());

            JSONObject transactionJs = helper.post(String.format("/orders/%s/transactions.json",shopifyOrderId),params);
            transaction = new Transaction((JSONObject)transactionJs.get("transaction"));
            if (!transaction.getStatus().equals("success")){
                throw new OrderConfirmFailure("Payment failure");
            }
        }

        return getBecknOrder(shopifyOrder);
    }


    @Override
    @SuppressWarnings("unchecked")
    public String getTrackingUrl(Order order) {
        String trackUrl = LocalOrderSynchronizer.getInstance().getTrackingUrl(order);
        if (trackUrl != null){
            return trackUrl;
        }

        JSONObject response = helper.get(String.format("/orders/%s.json", LocalOrderSynchronizer.getInstance().getLocalOrderId(order)), new JSONObject());
        ShopifyOrder eCommerceOrder = new ShopifyOrder((JSONObject) response.get("order"));
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
           LocalOrderSynchronizer.getInstance().setTrackingUrl(getBecknTransactionId(eCommerceOrder),trackUrl);
        }
        return trackUrl;
    }

    @Override
    @SuppressWarnings("unchecked")

    public Order cancel(Order order ) {

        JSONObject params = new JSONObject();
        params.put("reason", "customer");


        JSONObject response = helper.post(String.format("/orders/%s/cancel.json", LocalOrderSynchronizer.getInstance().getLocalOrderId(order)), params);
        String error = (String)response.get("error");
        if (!ObjectUtil.isVoid(error)) {
            throw new GenericBusinessError(error);
        }
        ShopifyOrder eCommerceOrder = new ShopifyOrder((JSONObject) response.get("order"));
        return getBecknOrder(eCommerceOrder);
    }

    @Override
    public Order getStatus(Order order) {

        /* Take status message and fill response with on_status message */
        JSONObject response = helper.get(String.format("/orders/%s.json", LocalOrderSynchronizer.getInstance().getLocalOrderId(order)), new JSONObject());
        ShopifyOrder eCommerceOrder = new ShopifyOrder((JSONObject) response.get("order"));

        return getBecknOrder(eCommerceOrder);
    }


    public String getSupportEmail() {
        return getStore().getSupportEmail();
    }


    @Override
    @SuppressWarnings("unchecked")
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
                location.getAddress().setName((String) store.get("name"));
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
                 //location.setTime(getProviderConfig().getTime());
                location.setDescriptor(new Descriptor());
                location.getDescriptor().setName(location.getAddress().getName());

                JSONObject meta = helper.get(String.format("/locations/%s/metafields.json",StringUtil.valueOf(store.get("id"))),new JSONObject());
                Map<String,String> locationMeta = new HashMap<>();
                JSONArray metafields = (JSONArray) meta.get("metafields");
                for (Object metafield : metafields){
                    JSONObject m = (JSONObject) metafield;
                    locationMeta.put((String)m.get("key"),(String)m.get("value"));
                }
                location.setGps(new GeoCoordinate(new BigDecimal(locationMeta.get("lat")),new BigDecimal(locationMeta.get("lng"))));
                locations.add(location);
            });
            return locations;
        });
    }

    @Override
    public Items getItems() {
        return cache.get(Items.class, () -> {
            Items items = new Items();
            Store store = getStore();

            InventoryLevels levels = getInventoryLevels();
            Cache<Long, List<Long>> inventoryLocations = new Cache<>(0, 0) {
                @Override
                protected List<Long> getValue(Long aLong) {
                    return new ArrayList<>();
                }
            };
            levels.forEach(level -> inventoryLocations.get(level.getInventoryItemId()).add(level.getLocationId()));
            Products products = getProducts();
            products.forEach(product -> {
                for (ProductVariant productVariant : product.getProductVariants()) {
                    inventoryLocations.get(productVariant.getInventoryItemId()); // Just load keys
                }
            });

            InventoryItems inventoryItems = getInventoryItems(inventoryLocations.keySet());

            Map<String, Double> taxRateMap = getTaxRateMap();

            for (Product product : products) {
                if (!product.getTagSet().contains("ondc")){
                    continue;
                }
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
                            descriptor.setName(variant.getTitle());
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
                        } else if (productImages.size() > 0) {
                            for (ProductImage i : productImages) {
                                descriptor.getImages().add(i.getSrc());
                            }
                        }
                        descriptor.setSymbol(descriptor.getImages().get(0));

                        item.setCategoryId(getProviderConfig().getCategory().getId());
                        item.setCategoryIds(new BecknStrings());
                        item.getCategoryIds().add(item.getCategoryId());
                        item.setTags(new Tags());

                        for (String tag: product.getTagSet()) {
                            item.getTags().set(tag, "true");
                        }
                        item.getTags().set("hsn_code", inventoryItem.getHarmonizedSystemCode());
                        item.getTags().set("country_of_origin", inventoryItem.getCountryCodeOfOrigin());
                        item.getTags().set("tax_rate", StringUtil.valueOf(taxRateMap.get(inventoryItem.getHarmonizedSystemCode())));
                        item.getTags().set("product_id", variant.getProductId());
                        if (product.isVeg()) {
                            item.getTags().set("veg", "yes");
                            item.getTags().set("non_veg", "no");
                        }else {
                            item.getTags().set("veg", "no");
                            item.getTags().set("non_veg", "yes");
                        }



                        item.setLocationId(BecknIdHelper.getBecknId(StringUtil.valueOf(location_id), getSubscriber(), Entity.provider_location));
                        item.setLocationIds(new BecknStrings());
                        item.getLocationIds().add(item.getLocationId());

                        /* Yhis should be abtracted out TODO */
                        if (getProviderConfig().getCategory().getDescriptor().getName().equals("Fruits and Vegetables")) {
                            item.setVeggiesFruits(new VeggiesFruits());
                            item.getVeggiesFruits().setNetQuantity(variant.getGrams() + " gms");
                        }else if (getProviderConfig().getCategory().getDescriptor().getName().equals("Packaged Foods")) {
                            item.setPrepackagedFood(new PrepackagedFood());
                            item.getPrepackagedFood().setNetQuantity(variant.getGrams() + " gms");
                            item.getPrepackagedFood().setBrandOwnerAddress(getProviderConfig().getLocation().getAddress().flatten());
                            item.getPrepackagedFood().setBrandOwnerName(getProviderConfig().getFulfillmentProviderName());
                            item.getPrepackagedFood().setBrandOwnerFSSAILicenseNo(getProviderConfig().getFssaiRegistrationNumber());
                        }else if (getProviderConfig().getCategory().getDescriptor().getName().equals("Packaged Commodities")) {
                            item.setPackagedCommodity(new PackagedCommodity());
                            item.getPackagedCommodity().setNetQuantityOrMeasureOfCommodityInPkg(variant.getGrams() + " gms");
                            item.getPackagedCommodity().setManufacturerOrPackerName(getProviderConfig().getFulfillmentProviderName());
                            item.getPackagedCommodity().setManufacturerOrPackerAddress(getProviderConfig().getLocation().getAddress().flatten());
                            item.getPackagedCommodity().setCommonOrGenericNameOfCommodity(product.getProductType());
                            //For statutory requirements show images.
                        }



                        Price price = new Price();
                        item.setPrice(price);
                        price.setMaximumValue(variant.getMrp());
                        price.setListedValue(variant.getPrice());
                        price.setCurrency(getStore().getCurrency());
                        price.setValue(variant.getPrice());
                        price.setOfferedValue(variant.getPrice());
                        price.setCurrency("INR");

                        item.setPaymentIds(new BecknStrings());
                        for (Payment payment : getSupportedPaymentCollectionMethods()) {
                            item.getPaymentIds().add(payment.getId()); //Only allow By BAP , ON_ORDER
                        }

                        item.setReturnable(getProviderConfig().isReturnSupported());
                        if (item.isReturnable()){
                            item.setReturnWindow(getProviderConfig().getReturnWindow());
                            item.setSellerPickupReturn(getProviderConfig().isReturnPickupSupported());
                        }
                        item.setCancellable(true);
                        item.setTimeToShip(getProviderConfig().getTurnAroundTime());
                        item.setAvailableOnCod(getProviderConfig().isCodSupported());

                        item.setContactDetailsConsumerCare(getProviderConfig().getLocation().getAddress().flatten() + " " + getProviderConfig().getSupportContact().flatten());
                        item.setFulfillmentIds(new BecknStrings());
                        for (Fulfillment fulfillment : getFulfillments()) {
                            item.getFulfillmentIds().add(fulfillment.getId());
                        }


                        items.add(item);

                    });
                }
            }
            return items;
        });
    }

    public Map<String,Double> getTaxRateMap(){
        return new Cache<>(0,0) {
            @Override
            protected Double getValue(String taxClass) {
                if (!ObjectUtil.isVoid(taxClass)){
                    AssetCode assetCode = AssetCode.find(taxClass);
                    return assetCode.getGstPct();
                }
                return 0.0D;
            }
        };
    }


    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
    private Products getProducts(){
        return cache.get(Products.class,()->{
            JSONObject input = new JSONObject();
            input.put("limit", 250);

            Products products = new Products();

            Page<JSONObject> productsPage = new Page<>();
            productsPage.next = "/products.json";
            while (productsPage.next != null) {
                productsPage = helper.getPage(productsPage.next, input);
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
        });
    }

    @SuppressWarnings("unchecked")
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
        JSONObject response = helper.get(String.format("/orders/%s.json", LocalOrderSynchronizer.getInstance().getLocalOrderId(order)), new JSONObject());
        ShopifyOrder eCommerceOrder = new ShopifyOrder((JSONObject) response.get("order"));
        FulfillmentStatusAdaptor adaptor = getFulfillmentStatusAdaptor() ;
        String transactionId = getBecknTransactionId(eCommerceOrder);

        if (adaptor != null){
            List<FulfillmentStatusAudit> audits = adaptor.getStatusAudit(String.valueOf(eCommerceOrder.getOrderNumber()));
            for (Iterator<FulfillmentStatusAudit> i = audits.iterator(); i.hasNext() ; ){
                FulfillmentStatusAudit audit = i.next();
                LocalOrderSynchronizer.getInstance().setFulfillmentStatusReachedAt(transactionId,audit.getFulfillmentStatus(),audit.getDate(),!i.hasNext());
            }
        }
        return LocalOrderSynchronizer.getInstance().getFulfillmentStatusAudit(transactionId);

    }

    public Order getBecknOrder(ShopifyOrder eCommerceOrder) {
        String transactionId = getBecknTransactionId(eCommerceOrder);
        LocalOrderSynchronizer.getInstance().setLocalOrderId(transactionId,eCommerceOrder.getId());
        Order lastReturnedOrderJson = LocalOrderSynchronizer.getInstance().getLastKnownOrder(transactionId);



        Order order = new Order();
        order.setPayment(new Payment());
        if (lastReturnedOrderJson.getPayment() != null) {
            order.getPayment().update(lastReturnedOrderJson.getPayment());
        }

        Fulfillment fulfillment = new Fulfillment();
        order.setFulfillment(fulfillment);
        if (lastReturnedOrderJson.getFulfillment() != null ){
            order.getFulfillment().setType(lastReturnedOrderJson.getFulfillment().getType());
        }
        if (ObjectUtil.isVoid(order.getFulfillment().getType())) {
            fulfillment.setType(FulfillmentType.home_delivery);
        }
        String fulfillmentId = "fulfillment/"+ fulfillment.getType()+"/"+transactionId;
        fulfillment.setId(fulfillmentId);

        eCommerceOrder.loadMetaFields(helper);


        setPayment(order.getPayment(),eCommerceOrder);
        order.getPayment().getParams().setTransactionId(transactionId);

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

        Quote quote = new Quote();
        order.setQuote(quote);
        quote.setTtl(15*60);
        quote.setPrice(new Price());
        quote.getPrice().setValue(order.getPayment().getParams().getAmount());
        quote.getPrice().setCurrency(order.getPayment().getParams().getCurrency());

        quote.setBreakUp(new BreakUp());


        Price productPrice = new Price(); productPrice.setValue(eCommerceOrder.getSubtotalPrice()); productPrice.setCurrency("INR");
        Price tax = new Price(); tax.setValue(eCommerceOrder.getTotalTax()); tax.setCurrency("INR");
        Price total = new Price(); total.setValue(eCommerceOrder.getTotalPrice());total.setCurrency("INR");
        Price shippingPrice = new Price();shippingPrice.setCurrency("INR");
        if (eCommerceOrder.getShippingLine() != null) {
            shippingPrice.setValue(eCommerceOrder.getShippingLine().getPrice());
        }


        BreakUpElement element = quote.getBreakUp().createElement(BreakUpCategory.tax,"Tax",tax);
        element.setItemId(fulfillmentId);
        quote.getBreakUp().add(element);

        if (shippingPrice.getValue() > 0) {
            element = quote.getBreakUp().createElement(BreakUpCategory.delivery, "Delivery Charges", shippingPrice);
            element.setItemId(fulfillmentId);
            quote.getBreakUp().add(element);
        }

        //Delivery is included


        setBilling(order,eCommerceOrder.getBillingAddress());
        //order.setId(meta.getBapOrderId());

        if (!ObjectUtil.isVoid(eCommerceOrder.getInvoiceUrl())) {
            order.setDocuments(new Documents());
            Document invoice = new Document();
            order.getDocuments().add(invoice);
            invoice.setLabel("invoice");
            invoice.setUrl(eCommerceOrder.getInvoiceUrl());
        }

        Status orderStatus = eCommerceOrder.getStatus();
        order.setState(orderStatus);
        order.setItems(new Items());

        eCommerceOrder.getLineItems().forEach(lineItem -> {
            Item item = createItemFromECommerceLineItem(lineItem);

            item.setFulfillmentId(lastReturnedOrderJson.getFulfillment().getId());
            BreakUpElement itemPrice = quote.getBreakUp().createElement(BreakUpCategory.item,item.getDescriptor().getName(),item.getPrice()); //This is line price and not unit price.
            itemPrice.setItemQuantity(item.getItemQuantity().getAllocated());
            itemPrice.setItem(item);
            itemPrice.setItemId(item.getId());
            item.setQuantity(item.getItemQuantity().getAllocated());

            quote.getBreakUp().add(itemPrice);
            order.getItems().add(item);
        });

        ShopifyOrder.Address shipping = eCommerceOrder.getShippingAddress();
        if (shipping == null || ObjectUtil.isVoid(shipping.getAddress1())){
            shipping = eCommerceOrder.getBillingAddress();
        }

        Locations locations = getProviderLocations();
        Location providerLocation = locations.get(BecknIdHelper.getBecknId(StringUtil.valueOf(eCommerceOrder.getLocationId()),getSubscriber(),Entity.provider_location));
        order.setProviderLocation(providerLocation);


        fulfillment.setStart(new FulfillmentStop());
        fulfillment.getStart().setLocation(providerLocation);
        fulfillment.setProviderId(String.format("%s/logistics",getSubscriber().getAppId()));


        fulfillment.setFulfillmentStatus(eCommerceOrder.getFulfillmentStatus());
        fulfillment.setEnd(new FulfillmentStop());
        if (lastReturnedOrderJson.getFulfillment().getEnd() != null) {
            fulfillment.getEnd().update(lastReturnedOrderJson.getFulfillment().getEnd());
        }
        fulfillment.getEnd().setLocation(new Location());
        if (fulfillment.getEnd().getLocation().getAddress() == null) {
            fulfillment.getEnd().getLocation().setAddress(shipping.getAddress());
        }

        fulfillment.getEnd().setContact(new Contact());
        fulfillment.getEnd().getContact().setPhone(shipping.getPhone());
        fulfillment.getEnd().getContact().setEmail(eCommerceOrder.getEmail());
        fulfillment.setContact(getProviderConfig().getSupportContact());


        Address address = fulfillment.getEnd().getLocation().getAddress();
        fulfillment.setCustomer(new User());
        fulfillment.getCustomer().setPerson(new Person());
        fulfillment.getCustomer().getPerson().setName(address.getName());

        order.setFulfillments( new in.succinct.beckn.Fulfillments());
        order.getFulfillments().add(fulfillment);

        order.setProvider(new Provider());
        order.getProvider().setId(getSubscriber().getAppId());
        order.getProvider().setDescriptor(new Descriptor());
        order.getProvider().getDescriptor().setName(getProviderConfig().getStoreName());
        order.getProvider().setCategoryId(getProviderConfig().getCategory().getId());


        order.setCreatedAt(eCommerceOrder.getCreatedAt());
        order.setUpdatedAt(eCommerceOrder.getUpdatedAt());


        return order;
    }

    private Item createItemFromECommerceLineItem(LineItem eCommerceLineItem) {
        Item item = new Item();
        item.setDescriptor(new Descriptor());
        item.setId(BecknIdHelper.getBecknId(String.valueOf(eCommerceLineItem.getVariantId()), getSubscriber(), Entity.item));
        item.getDescriptor().setName(eCommerceLineItem.getName());
        item.getDescriptor().setCode(eCommerceLineItem.getSku());
        if (ObjectUtil.isVoid(item.getDescriptor().getCode())){
            item.getDescriptor().setCode(item.getDescriptor().getName());
        }
        item.getDescriptor().setLongDesc(item.getDescriptor().getName());
        item.getDescriptor().setShortDesc(item.getDescriptor().getName());

        in.succinct.bpp.search.db.model.Item dbItem = getItem(item.getId());

        Item itemIndexed  = new Item(dbItem.getObjectJson());
        item.setTags(itemIndexed.getTags());
        item.setCategoryId(itemIndexed.getCategoryId());
        item.setCategoryIds(itemIndexed.getCategoryIds());

        ItemQuantity itemQuantity = new ItemQuantity();
        itemQuantity.setAllocated(new Quantity());
        itemQuantity.getAllocated().setCount(doubleTypeConverter.valueOf(eCommerceLineItem.getQuantity()).intValue());

        item.setItemQuantity(itemQuantity);

        Price price = new Price();
        item.setPrice(price);

        //price.setListedValue(doubleTypeConverter.valueOf(eCommerceLineItem.get("subtotal")));
        price.setValue(doubleTypeConverter.valueOf(eCommerceLineItem.getPrice()));
        price.setCurrency("INR");


        return item;

    }

    private boolean isPaid(ShopifyOrder shopifyOrder){
        if ("paid".equals(shopifyOrder.getFinancialStatus())){
            return true;
        }
        PaymentTerms terms = shopifyOrder.getPaymentTerms();

        if (terms == null){
            return false;
        }
        double toPay = terms.getAmount();

        PaymentSchedules schedules = terms.getPaymentSchedules();
        Bucket paid  = new Bucket();
        for (PaymentSchedule schedule : schedules){
            if (schedule.getCompletedAt() != null ) {
                paid.increment(schedule.getAmount());
            }
        }
        return DoubleUtils.compareTo(paid.doubleValue(), toPay)>= 0;
    }



    private void setPayment(Payment payment, ShopifyOrder eCommerceOrder) {
        payment.setSettlementBasis(SettlementBasis.Collection);

        if (payment.getCollectedBy() == null){
            if (getProviderConfig().isCodSupported()){
                payment.setCollectedBy(CollectedBy.BPP);
                payment.setType(PaymentType.POST_FULFILLMENT);
            }else {
                payment.setCollectedBy(CollectedBy.BAP);
                payment.setType(PaymentType.ON_ORDER);
            }
        }else if (payment.getCollectedBy() == CollectedBy.BPP){
            payment.setType(PaymentType.POST_FULFILLMENT);
        }else{
            payment.setCollectedBy(CollectedBy.BAP);
            payment.setType(PaymentType.ON_ORDER);
        }

        payment.setSettlementDetails(new SettlementDetails());
        SettlementDetail detail = new SettlementDetail();
        payment.getSettlementDetails().add(detail);
        detail.setUpiAddress(getProviderConfig().getVPA());
        detail.setSettlementType(SettlementType.UPI);
        detail.setSettlementPhase(SettlementPhase.SALE_AMOUNT);
        detail.setSettlementCounterparty(SettlementCounterparty.SELLER_APP);
        detail.setSettlementStatus(PaymentStatus.NOT_PAID);
        boolean paid = isPaid(eCommerceOrder);
        boolean isSettled = eCommerceOrder.isSettled();

        if (payment.getCollectedBy() == CollectedBy.BPP){
            if (paid) {
                payment.setStatus(PaymentStatus.PAID);
                detail.setSettlementStatus(isSettled ? PaymentStatus.PAID : PaymentStatus.NOT_PAID);
            }else {
                payment.setStatus(PaymentStatus.NOT_PAID);
                detail.setSettlementStatus(PaymentStatus.NOT_PAID);
            }
        }else {
            if (paid){
                payment.setStatus(PaymentStatus.PAID);
                detail.setSettlementStatus(isSettled ? PaymentStatus.PAID : PaymentStatus.NOT_PAID);
            }
        }


        payment.setParams(new Params());
        payment.getParams().setCurrency(getStore().getCurrency());
        if (eCommerceOrder.getPaymentTerms() != null) {
            payment.getParams().setAmount(doubleTypeConverter.valueOf(eCommerceOrder.getPaymentTerms().getAmount()));
        }else {
            payment.getParams().setAmount(eCommerceOrder.getTotalPrice());
        }

    }

    private void setBilling(Order target, ShopifyOrder.Address source) {
        Billing billing = new Billing();
        target.setBilling(billing);

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
        billing.setPhone(source.getPhone());
    }

}
