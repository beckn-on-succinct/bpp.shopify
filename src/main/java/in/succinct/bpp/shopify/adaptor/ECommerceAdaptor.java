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
import in.succinct.beckn.SettlementDetail;
import in.succinct.beckn.SettlementDetail.SettlementCounterparty;
import in.succinct.beckn.SettlementDetail.SettlementPhase;
import in.succinct.beckn.SettlementDetail.SettlementType;
import in.succinct.beckn.SettlementDetails;
import in.succinct.beckn.Tag;
import in.succinct.beckn.Tags;
import in.succinct.beckn.User;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.TimeSensitiveCache;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper.Entity;
import in.succinct.bpp.core.db.model.BecknOrderMeta;
import in.succinct.bpp.core.db.model.ProviderConfig.Serviceability;
import in.succinct.bpp.shopify.adaptor.ECommerceSDK.Page;
import in.succinct.bpp.shopify.model.DraftOrder;
import in.succinct.bpp.shopify.model.DraftOrder.Fulfillments;
import in.succinct.bpp.shopify.model.DraftOrder.LineItem;
import in.succinct.bpp.shopify.model.DraftOrder.LineItems;
import in.succinct.bpp.shopify.model.DraftOrder.NoteAttributes;
import in.succinct.bpp.shopify.model.DraftOrder.PaymentSchedule;
import in.succinct.bpp.shopify.model.DraftOrder.PaymentSchedules;
import in.succinct.bpp.shopify.model.DraftOrder.PaymentTerms;
import in.succinct.bpp.shopify.model.DraftOrder.ShippingLine;

import in.succinct.bpp.shopify.model.ProductImages;
import in.succinct.bpp.shopify.model.ProductImages.ProductImage;
import in.succinct.bpp.shopify.model.Products;
import in.succinct.bpp.shopify.model.Products.InventoryItem;
import in.succinct.bpp.shopify.model.Products.InventoryItems;
import in.succinct.bpp.shopify.model.Products.InventoryLevel;
import in.succinct.bpp.shopify.model.Products.InventoryLevels;
import in.succinct.bpp.shopify.model.Products.Metafield;
import in.succinct.bpp.shopify.model.Products.Metafields;
import in.succinct.bpp.shopify.model.Products.Product;
import in.succinct.bpp.shopify.model.Products.ProductVariant;
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



    public Order initializeDraftOrder(Request request) {
        DraftOrder draftOrder = new DraftOrder();
        Order bo = request.getMessage().getOrder();
        fixFulfillment(request.getContext(), bo);
        fixLocation(bo);
        Fulfillment f = bo.getFulfillment();
        Location storeLocation = bo.getProviderLocation();

        Serviceability serviceability = f.getEnd() == null ? null : getProviderConfig().getServiceability(f.getType(),f.getEnd(),storeLocation);
        if (serviceability != null && !serviceability.isServiceable()){
            throw new RuntimeException("Quote provided was invalidated.");
        }



        BecknOrderMeta orderMeta = Database.getTable(BecknOrderMeta.class).newRecord();
        orderMeta.setBecknTransactionId(request.getContext().getTransactionId());
        orderMeta = Database.getTable(BecknOrderMeta.class).getRefreshed(orderMeta);


        if (!ObjectUtil.isVoid(bo.getId())) {
            orderMeta.setBapOrderId(bo.getId());// Sent from bap.!
        }

        if (!orderMeta.getRawRecord().isNewRecord() && !ObjectUtil.isVoid(orderMeta.getECommerceDraftOrderId())) {
            draftOrder.setId(orderMeta.getECommerceDraftOrderId());
        }
        draftOrder.setEmail(bo.getFulfillment().getEnd().getContact().getEmail());
        draftOrder.setCurrency("INR");
        draftOrder.setSource("beckn");
        draftOrder.setName("beckn-" + request.getContext().getTransactionId());
        draftOrder.setNoteAttributes(new NoteAttributes());

        for (String key : new String[]{"bap_id", "bap_uri", "domain", "transaction_id", "city", "country", "core_version"}) {
            Tag meta = new Tag();
            meta.setName(String.format("context.%s", key));
            meta.setValue(request.getContext().get(key));
            draftOrder.getNoteAttributes().add(meta);
        }

        if (!ObjectUtil.isVoid(draftOrder.getId())) {
            delete(draftOrder);
        }

        Order lastKnown = new Order(orderMeta.getOrderJson());
        lastKnown.setFulfillments(bo.getFulfillments());
        lastKnown.setProviderLocation(bo.getProviderLocation());
        lastKnown.setItems(bo.getItems());
        lastKnown.setFulfillment(bo.getFulfillment());
        if (lastKnown.getFulfillments().size() > 0) {
            lastKnown.setFulfillment(lastKnown.getFulfillments().get(0));
        }
        lastKnown.setBilling(bo.getBilling());
        lastKnown.setPayment(bo.getPayment());
        orderMeta.setOrderJson(lastKnown.getInner().toString()); //.getInner() needed to avoid getting the initialized string payload back.
        if (bo.getPayment() != null && bo.getPayment().getBuyerAppFinderFeeType() != null){
            orderMeta.setBuyerAppFinderFeeType(bo.getPayment().getBuyerAppFinderFeeType().toString());
            orderMeta.setBuyerAppFinderFeeAmount(doubleTypeConverter.valueOf(bo.getPayment().getBuyerAppFinderFeeAmount()));
        }
        setShipping( bo.getFulfillment(), draftOrder);

        if (bo.getBilling() == null){
            bo.setBilling(new Billing());
        }
        if (bo.getBilling().getAddress() == null){
            bo.getBilling().setAddress(bo.getFulfillment().getEnd().getLocation().getAddress());
        }

        setBilling( bo.getBilling(),draftOrder);

        if (serviceability != null) {
            ShippingLine shippingLine = new ShippingLine();
            shippingLine.setTitle("Standard");
            shippingLine.setPrice(serviceability.getCharges());
            draftOrder.setShippingLine(shippingLine);
        }


        if (bo.getItems() != null) {
            bo.getItems().forEach(boItem -> {
                in.succinct.bpp.search.db.model.Item dbItem = getItem(boItem.getId());
                Item refreshedBoItem = new Item(dbItem.getObjectJson());
                JSONObject inspectQuantity = (JSONObject) boItem.getInner().get("quantity");
                if (inspectQuantity.containsKey("count")){
                    refreshedBoItem.setQuantity(boItem.getQuantity());
                }else {
                    refreshedBoItem.setItemQuantity(boItem.getItemQuantity());
                }
                addItem(draftOrder, refreshedBoItem);
            });
        }

        return saveDraftOrder(draftOrder, orderMeta);
    }

    private void delete(DraftOrder draftOrder) {
        helper.delete(String.format("/draft_orders/%s.json",draftOrder.getId()) , new JSONObject());
        draftOrder.rm("id");
    }

    private in.succinct.bpp.search.db.model.Item getItem(String objectId) {

        Select select = new Select().from(in.succinct.bpp.search.db.model.Item.class);
        List<in.succinct.bpp.search.db.model.Item> dbItems = select.where(new Expression(select.getPool(), Conjunction.AND).
                add(new Expression(select.getPool(), "APPLICATION_ID", Operator.EQ, getApplication().getId())).
                add(new Expression(select.getPool(), "OBJECT_ID", Operator.EQ, objectId))).execute(1);

        in.succinct.bpp.search.db.model.Item dbItem = dbItems.isEmpty() ? null : dbItems.get(0);
        return dbItem;
    }

    private void addItem(DraftOrder draftOrder, Item item) {
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

    }

    private TypeConverter<Double> doubleTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter();
    private TypeConverter<Boolean> booleanTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(boolean.class).getTypeConverter();

    public void setBilling(Billing source,DraftOrder target) {

        if (source == null) {
            return;
        }
        DraftOrder.Address billing = new DraftOrder.Address();
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

    public void setShipping(Fulfillment source,DraftOrder target) {
        if (source == null) {
            return;
        }
        DraftOrder.Address shipping = new DraftOrder.Address();
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
        //shipping.put("email",contact.getEmail());
        shipping.setPhone(contact.getPhone());
        if (gps != null) {
            shipping.setLatitude(gps.getLat().doubleValue());
            shipping.setLongitude(gps.getLng().doubleValue());
        }
    }

    public Order saveDraftOrder(DraftOrder draftOrder,   BecknOrderMeta orderMeta) {
        JSONObject dro = new JSONObject();
        dro.put("draft_order",draftOrder.getInner());

        JSONObject outOrder = helper.post("/draft_orders.json", dro);
        DraftOrder oDraftOrder = new DraftOrder((JSONObject) outOrder.get("draft_order"));

        orderMeta.setECommerceDraftOrderId(oDraftOrder.getId());
        orderMeta.save();

        return getBecknOrder(oDraftOrder,orderMeta);

    }

    public Order confirmDraftOrder(Order inOrder,BecknOrderMeta orderMeta) {

        //Update with latest attributes.
        JSONObject parameter = new JSONObject();
        parameter.put("payment_pending", inOrder.getPayment().getStatus() != PaymentStatus.PAID);
        JSONObject response = helper.putViaPost(String.format("/draft_orders/%s/complete.json", orderMeta.getECommerceDraftOrderId()), parameter); //Marks payment as paid!

        DraftOrder draftOrder = new DraftOrder((JSONObject) response.get("draft_order"));

        orderMeta.setECommerceOrderId(draftOrder.getOrderId());
        if (ObjectUtil.isVoid(orderMeta.getBapOrderId())) {
            orderMeta.setBapOrderId(BecknIdHelper.getBecknId(orderMeta.getECommerceOrderId(), getSubscriber(), Entity.order));
        }
        orderMeta.save();

        response = helper.get(String.format("/orders/%s.json", draftOrder.getOrderId()), new JSONObject());


        in.succinct.bpp.shopify.model.Order order = new in.succinct.bpp.shopify.model.Order((JSONObject) response.get("order"));


        return getBecknOrder(order);
    }


    @Override
    public String getTrackingUrl(Order order) {
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);


        JSONObject params = new JSONObject();
        params.put("fields", "tracking_urls");

        JSONObject fulfillmentJson = helper.get(String.format("/orders/%s/fulfillments.json", meta.getECommerceOrderId()), params);
        Fulfillments fulfillments = new Fulfillments((JSONArray) fulfillmentJson.get("fulfillments"));
        String url = null;
        if (fulfillments.size() > 0 ) {
            DraftOrder.Fulfillment fulfillment = fulfillments.get(0);
            BecknStrings urls = fulfillment.getTrackingUrls();
            if (urls.size() >0 ){
                url = urls.get(0);
            }
        }
        return url;
    }

    @Override
    public Order cancel(Order order) {
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        JSONObject params = new JSONObject();
        params.put("reason", "customer");


        JSONObject response = helper.post(String.format("/orders/%s/cancel.json", meta.getECommerceOrderId()), params);
        String error = (String)response.get("error");
        if (!ObjectUtil.isVoid(error)) {
            throw new RuntimeException(error);
        }
        in.succinct.bpp.shopify.model.Order eCommerceOrder = new in.succinct.bpp.shopify.model.Order((JSONObject) response.get("order"));
        Order outOrder = getBecknOrder(eCommerceOrder);
        return outOrder;
    }

    @Override
    public Order getStatus(Order order) {
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        /* Take status message and fill response with on_status message */
        JSONObject response = helper.get(String.format("/orders/%s.json", meta.getECommerceOrderId()), new JSONObject());
        in.succinct.bpp.shopify.model.Order eCommerceOrder = new in.succinct.bpp.shopify.model.Order((JSONObject) response.get("order"));

        return getBecknOrder(eCommerceOrder);
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
                location.getAddress().setName((String) store.get("name"));
                location.getAddress().setStreet((String)store.get("address1"));
                location.getAddress().setLocality((String)store.get("address2"));
                location.getAddress().setCity((String) store.get("city"));
                location.getAddress().setPinCode((String) store.get("zip"));
                location.getAddress().setCountry((String) store.get("country"));
                location.getAddress().setState((String) store.get("province"));
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
                        StringTokenizer tokenizer = new StringTokenizer(product.getTags(), ",");
                        while (tokenizer.hasMoreTokens()) {
                            item.getTags().set(tokenizer.nextToken().trim(), "true");
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

                        /** Yhis should be abtracted out TODO */
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
                            item.getPackagedCommodity().setManufacturerOfPackerName(getProviderConfig().getFulfillmentProviderName());
                            item.getPackagedCommodity().setManufacturerOfPackerAddress(getProviderConfig().getLocation().getAddress().flatten());
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

                        /*
                        PackagedCommodity packagedCommodity = new PackagedCommodity();

                        item.setPackagedCommodity(packagedCommodity);
                        packagedCommodity.setManufacturerOfPackerAddress(item.getContactDetailsConsumerCare());
                        packagedCommodity.setManufacturerOfPackerName(getProviderConfig().getStoreName());
                        packagedCommodity.setCommonOrGenericNameOfCommodity(product.getProductType());
                        packagedCommodity.setNetQuantityOrMeasureOfCommodityInPkg(variant.getGrams() + " Grams");
                        */

                        //For statutory requirements show images.

                        items.add(item);

                    });
                }
            }
            return items;
        });
    }

    public Map<String,Double> getTaxRateMap(){
        Map<String,Double> taxRateMap = new Cache<>(0,0) {
            @Override
            protected Double getValue(String taxClass) {
                if (!ObjectUtil.isVoid(taxClass)){
                    AssetCode assetCode = AssetCode.find(taxClass);
                    return assetCode.getGstPct();
                }
                return 0.0D;
            }
        };
        return taxRateMap;
    }


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
                    inventoryLevels.forEach(o -> {
                        levels.add(new InventoryLevel((JSONObject) o));
                    });
                }else {
                    break;
                }
            }
            return levels;
        });
    }
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
                    inventoryItemsArray.forEach(o -> {
                        inventoryItems.add(new InventoryItem((JSONObject) o));
                    });
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

    public Order getBecknOrder(DraftOrder eCommerceOrder) {

        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        if (eCommerceOrder instanceof in.succinct.bpp.shopify.model.Order) {
            meta.setECommerceOrderId(eCommerceOrder.getId());
        }else {
            meta.setECommerceDraftOrderId(eCommerceOrder.getId());
        }
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        return getBecknOrder(eCommerceOrder,meta);
    }
    public Order getBecknOrder(DraftOrder eCommerceOrder,BecknOrderMeta meta) {

        Order lastReturnedOrderJson = new Order(meta.getOrderJson());
        String fulfillmentId = "fulfillment/"+ FulfillmentType.home_delivery+"/"+meta.getBecknTransactionId();
        Order order = new Order();
        order.setPayment(new Payment());
        if (lastReturnedOrderJson.getPayment() != null) {
            order.getPayment().update(lastReturnedOrderJson.getPayment());
        }

        //if (order.getPayment().getStatus() != PaymentStatus.PAID){

            setPayment(order.getPayment(),eCommerceOrder);
            order.getPayment().getParams().setTransactionId(meta.getBecknTransactionId());
        //}
        if (lastReturnedOrderJson.getPayment() != null && lastReturnedOrderJson.getPayment().getBuyerAppFinderFeeAmount() != null ) {
            order.getPayment().setBuyerAppFinderFeeAmount(lastReturnedOrderJson.getPayment().getBuyerAppFinderFeeAmount());
            order.getPayment().setBuyerAppFinderFeeType(lastReturnedOrderJson.getPayment().getBuyerAppFinderFeeType());
        }else {
            order.getPayment().setBuyerAppFinderFeeAmount(meta.getBuyerAppFinderFeeAmount());
            order.getPayment().setBuyerAppFinderFeeType(CommissionType.valueOf(meta.getBuyerAppFinderFeeType()));
        }
        Double feeAmount = order.getPayment().getBuyerAppFinderFeeAmount();
        if (feeAmount != null) {
            if (order.getPayment().getBuyerAppFinderFeeType() == CommissionType.Percent) {
                if (feeAmount > getProviderConfig().getMaxAllowedCommissionPercent()) {
                    throw new RuntimeException("Max commission percent exceeded");
                }
            } else {
                double pct = new DoubleHolder(100.0 * feeAmount / order.getPayment().getParams().getAmount(), 2).getHeldDouble().doubleValue();
                if (pct > getProviderConfig().getMaxAllowedCommissionPercent()) {
                    throw new RuntimeException("Max commission percent exceeded");
                }
            }
        }else {
            //Hard coded but should pass from search
            order.getPayment().setBuyerAppFinderFeeType(CommissionType.Percent);
            order.getPayment().setBuyerAppFinderFeeAmount(getProviderConfig().getMaxAllowedCommissionPercent());
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

        /* BreakUpElement element = quote.getBreakUp().createElement(BreakUpCategory.item,"Total Product",productPrice);
        quote.getBreakUp().add(element);

         */
        BreakUpElement element = quote.getBreakUp().createElement(BreakUpCategory.tax,"Tax",tax);
        element.setItemId(fulfillmentId);
        quote.getBreakUp().add(element);

        if (shippingPrice.getValue() > 0) {
            element = quote.getBreakUp().createElement(BreakUpCategory.delivery, "Delivery Charges", shippingPrice);
            element.setItemId(fulfillmentId);
            quote.getBreakUp().add(element);
        }

        /* element = quote.getBreakUp().createElement(BreakUpCategory.item,"Total",total);
        quote.getBreakUp().add(element);
        */


        //Delivery is included


        setBilling(order,eCommerceOrder.getBillingAddress());
        //order.setId(meta.getBapOrderId());
        Status orderStatus = eCommerceOrder.getStatus();
        if (orderStatus == null && eCommerceOrder instanceof in.succinct.bpp.shopify.model.Order){
            if (eCommerceOrder.getCancelledAt() != null){
                orderStatus = Status.Cancelled;
            }else if (eCommerceOrder.getCompletedAt() != null){
                orderStatus = Status.Completed;
            }else {
                orderStatus = Status.Accepted;
            }
        }
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

        DraftOrder.Address shipping = eCommerceOrder.getShippingAddress();
        if (ObjectUtil.isVoid(shipping.getAddress1())){
            shipping = eCommerceOrder.getBillingAddress();
        }

        Locations locations = getProviderLocations();
        Location providerLocation = locations.get(lastReturnedOrderJson.getProviderLocation().getId());
        order.setProviderLocation(providerLocation);

        Fulfillment fulfillment = new Fulfillment();
        order.setFulfillment(fulfillment);
        fulfillment.setId(fulfillmentId);
        fulfillment.setStart(new FulfillmentStop());
        fulfillment.getStart().setLocation(providerLocation);
        fulfillment.setProviderId(String.format("%s/logistics",getSubscriber().getAppId()));
        fulfillment.setType(FulfillmentType.home_delivery);


        fulfillment.setFulfillmentStatus(Fulfillment.getFulfillmentStatus(orderStatus));
        fulfillment.setEnd(new FulfillmentStop());
        if (lastReturnedOrderJson.getFulfillment().getEnd() != null) {
            fulfillment.getEnd().update(lastReturnedOrderJson.getFulfillment().getEnd());
        }
        if (fulfillment.getEnd().getLocation() == null) {
            fulfillment.getEnd().setLocation(new Location());
        }
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

        if (!ObjectUtil.isVoid(meta.getBapOrderId())) {
            order.setId(meta.getBapOrderId());
        }
        order.setCreatedAt(eCommerceOrder.getCreatedAt());
        order.setUpdatedAt(eCommerceOrder.getUpdatedAt());
        /*
        order.setDocuments( new Documents());
        Document invoice = new Document();
        order.getDocuments().add(invoice);
        invoice.setLabel("invoice");
        invoice.setUrl(""); // Shopify doesnot give any url right now , Even Data url seems to be iffy

         */

        meta.setOrderJson(order.toString());
        meta.save();


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

    private boolean isPaid(PaymentTerms terms){
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



    private void setPayment(Payment payment, DraftOrder eCommerceOrder) {
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
        boolean paid = isPaid(eCommerceOrder.getPaymentTerms());
        boolean isSettled = false;
        if (eCommerceOrder instanceof in.succinct.bpp.shopify.model.Order){
            JSONObject meta = helper.get(String.format("/orders/%s/metafields.json",StringUtil.valueOf(eCommerceOrder.getId())),new JSONObject());
            JSONArray metafieldArray = (JSONArray) meta.get("metafields");
            Metafields metafields = new Metafields(metafieldArray);
            for (Metafield m : metafields){
                if (m.getKey().equals("settled")) {
                    isSettled = Database.getJdbcTypeHelper("").getTypeRef(Boolean.class).getTypeConverter().valueOf(m.getValue());
                    break;
                }
            }
        }




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
                detail.setSettlementStatus(PaymentStatus.PAID);
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

    private void setBilling(Order target, DraftOrder.Address source) {
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
