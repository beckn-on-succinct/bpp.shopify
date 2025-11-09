package in.succinct.bpp.shopify.adaptor;

import com.venky.cache.Cache;
import com.venky.core.date.DateUtils;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.geo.GeoCoordinate;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import com.venky.swf.plugins.collab.db.model.config.Country;
import com.venky.swf.plugins.collab.db.model.participants.admin.Facility;
import com.venky.swf.plugins.gst.db.model.assets.AssetCode;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.Address;
import in.succinct.beckn.BecknException;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Billing;
import in.succinct.beckn.Cancellation;
import in.succinct.beckn.Cancellation.CancelledBy;
import in.succinct.beckn.CancellationReasons.CancellationReasonCode;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Circle;
import in.succinct.beckn.City;
import in.succinct.beckn.Contact;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Error;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.Fulfillment.RetailFulfillmentType;
import in.succinct.beckn.FulfillmentStop;
import in.succinct.beckn.Fulfillments;
import in.succinct.beckn.Image;
import in.succinct.beckn.Images;
import in.succinct.beckn.Invoice;
import in.succinct.beckn.Invoice.Invoices;
import in.succinct.beckn.Item;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Message;
import in.succinct.beckn.Option;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.Status;
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payment.CollectedBy;
import in.succinct.beckn.Payment.Params;
import in.succinct.beckn.Payment.PaymentStatus;
import in.succinct.beckn.Payment.PaymentTransaction;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Person;
import in.succinct.beckn.Price;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Request;
import in.succinct.beckn.Scalar;
import in.succinct.beckn.SellerException;
import in.succinct.beckn.SellerException.NoDataAvailable;
import in.succinct.beckn.State;
import in.succinct.beckn.TagGroup;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.core.db.model.User;
import in.succinct.bpp.shopify.adaptor.ECommerceSDK.InventoryLocations;
import in.succinct.bpp.shopify.db.model.Company;
import in.succinct.bpp.shopify.model.ProductImages;
import in.succinct.bpp.shopify.model.ProductImages.ProductImage;
import in.succinct.bpp.shopify.model.Products;
import in.succinct.bpp.shopify.model.Products.InventoryItem;
import in.succinct.bpp.shopify.model.Products.InventoryItems;
import in.succinct.bpp.shopify.model.Products.Product;
import in.succinct.bpp.shopify.model.Products.ProductVariant;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import in.succinct.bpp.shopify.model.ShopifyOrder.LineItem;
import in.succinct.bpp.shopify.model.ShopifyOrder.LineItems;
import in.succinct.bpp.shopify.model.ShopifyOrder.NoteAttributes;
import in.succinct.bpp.shopify.model.ShopifyOrder.TaxLine;
import in.succinct.bpp.shopify.model.ShopifyOrder.TaxLines;
import in.succinct.bpp.shopify.model.ShopifyOrder.Transaction;
import in.succinct.bpp.shopify.model.Store;
import in.succinct.onet.core.adaptor.NetworkAdaptor.Domain;
import in.succinct.onet.core.adaptor.NetworkAdaptor.DomainCategory;
import in.succinct.onet.core.adaptor.NetworkAdaptorFactory;
import in.succinct.onet.core.api.BecknIdHelper;
import in.succinct.onet.core.api.BecknIdHelper.Entity;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ECommerceAdaptor extends CommerceAdaptor {
    
    public ECommerceAdaptor(Map<String, String> configuration, Subscriber subscriber) {
        super(configuration, subscriber);
    }
    
    protected Set<String> getCredentialAttributes() {
        return new HashSet<>() {{
            add("X-Shopify-Access-Token");
            add("X-Store-Url");
        }};
    }
    
    
    public Map<String, String> getCredentials(User user) {
        return user == null ? new HashMap<>() : user.getCredentials(true, getCredentialAttributes());
    }
    
    
    private void setErrorResponse(Request response, Exception ex) {
        response.setError(new Error() {{
            if (ex instanceof BecknException) {
                setCode(((BecknException) ex).getErrorCode());
            } else {
                setCode(new SellerException.GenericBusinessError().getErrorCode());
            }
            setMessage(ex.getMessage());
        }});
    }
    
    
    @Override
    public void search(Request request, Request response) {
        Domain domain = NetworkAdaptorFactory.getInstance().getAdaptor(request.getContext().getNetworkId()).getDomains().get(request.getContext().getDomain());
        
        DomainCategory allowedDomainCategory = getProviderConfig().getDomainCategory();
        if (domain.getDomainCategory() != allowedDomainCategory) {
            setErrorResponse(response, new NoDataAvailable());
            return;
        }
        response.setMessage(new Message() {{
            setCatalog(new Catalog());
        }});
        //Search is served from network search engine.
    }
    
    @Override
    public void confirm(Request request, Request response) {
        User user = getUser(request);
        if (user == null || ObjectUtil.isVoid(getCredentials(user))) {
            throw new RuntimeException("User Not Authorized");
        }
        ECommerceSDK helper  = new ECommerceSDK(user.getCredentials(true,getCredentialAttributes()));
        ShopifyOrder appResponse = save(request,true);
        if (response.getMessage() == null){
            response.setMessage(new Message());
        }
        response.getMessage().setOrder(convert(helper,appResponse));
    }
    
    @Override
    public void track(Request request, Request response) {
        Order order = request.getMessage().getOrder();
        User user = getUser(request);
        
        ECommerceSDK helper = new ECommerceSDK(user.getCredentials(true,getCredentialAttributes()));
        ShopifyOrder latest = findShopifyOrder(helper,order);
        Order current = convert(helper,latest);
        if (response.getMessage() == null){
            response.setMessage(new Message());
        }
        response.getMessage().setOrder(current);
        
    }
    
    @Override
    public void cancel(Request request, Request response) {
        throw new UnsupportedOperationException();
    }
    
    
    // Mey not need to modify below this line..
    
    
    
    @Override
    public void select(Request request, Request response) {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public void init(Request request, Request response) {
        response.update(request);
        response.getExtendedAttributes().setInner(request.getExtendedAttributes().getInner());
    }
    
    
    @Override
    public void update(Request request, Request response) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void status(Request request, Request response) {
        track(request, response);
    }
    
    @Override
    public void _search(String providerId, Request reply) {
        super._search(providerId, reply);
        reply.setMessage(new Message() {{
            setCatalog(new Catalog() {{
                setDescriptor(new Descriptor(){{
                    setCode(Config.instance().getHostName());
                    setName(getCode());
                    setShortDesc(getCode());
                    setLongDesc(getCode());
                }});
                
                setProviders(new Providers());
                Select select = new Select().from(User.class);
                if (!ObjectUtil.isVoid(providerId)) {
                    select.where(new Expression(select.getPool(), "PROVIDER_ID", Operator.EQ, providerId));
                } else {
                    select.where(new Expression(select.getPool(), "PROVIDER_ID", Operator.NE));
                }
                for (User u : select.execute(User.class)) {
                    getProviders().add(getProvider(u));
                }
            }});
        }});
    }
    
    public static final String PREPAID = "PRE-PAID";
    public static final String POST_DELIVERY = "POST-DELIVERY";
    
    private Provider getProvider(User u) {
        Provider provider = new Provider();
        provider.setId(u.getProviderId());
        
        List<com.venky.swf.plugins.collab.db.model.participants.admin.Company> companies = u.getCompanies();
        if (!companies.isEmpty()) {
            Company company = companies.get(0).getRawRecord().getAsProxy(Company.class);
            provider.setDescriptor(new Descriptor() {{
                setName(company.getName());
                setLongDesc(company.getName());
                setShortDesc(company.getName());
                if (!ObjectUtil.isVoid(company.getTaxIdentificationNumber())) {
                    setCode(company.getTaxIdentificationNumber());
                }
            }});
            provider.setLocations(getLocations(company));
            provider.setFulfillments(getFulfillments(company));
            Facility facility = company.getFacilities().get(0);
            for (Fulfillment fulfillment : provider.getFulfillments()) {
                fulfillment.setContact(new Contact(){{
                    setEmail(facility.getEmail());
                    setPhone(facility.getPhoneNumber());
                }});
                fulfillment.setProviderId(provider.getId());
                fulfillment.setProviderName(provider.getDescriptor().getName());
                fulfillment.setTracking(false);
                FulfillmentStop start = fulfillment._getStart();
                if (start == null && !provider.getLocations().isEmpty()){
                    start = new FulfillmentStop();
                    start.setLocation(provider.getLocations().get(0));
                    fulfillment._setStart(start);
                }
                if (start != null) {
                    start.setContact(fulfillment.getContact());
                    start.setPerson(new Person(){{
                        setName(u.getLongName());
                    }});
                }
            }
            
            provider.setPayments(getPayments(company));
            provider.setCategories(new Categories());
            
            provider.setItems(getItems(provider,new ECommerceSDK(u.getCredentials(true,getCredentialAttributes()))));
            
            TypeConverter<Boolean> converter =  company.getReflector().getJdbcTypeHelper().getTypeRef(boolean.class).getTypeConverter();
            provider.setTag("kyc","tax_id",company.getTaxIdentificationNumber());
            provider.setTag("kyc","owner_name",!ObjectUtil.isVoid(company.getCompanyOwnerName()) ?
                    company.getCompanyOwnerName() :
                    !ObjectUtil.isVoid(company.getAccountHolderName()) ?
                            company.getAccountHolderName() :
                            u.getName());
            provider.setTag("kyc","registration_id",company.getRegistrationNumber());
            provider.setTag("kyc","complete",converter.toString(ObjectUtil.equals(company.getVerificationStatus(), "APPROVED")));
            provider.setTag("kyc","ok",converter.toString(company.isKycOK()));
            
            provider.setTag("network","environment",company.getNetworkEnvironment());
            provider.setTag("network","suspended",converter.toString(company.isSuspended()));
            
        }
        return provider;
    }
    
    private Payments getPayments(Company company) {
        return new Payments() {{
            if (company.isPrepaidSupported()) {
                Payment prepaid = new Payment();
                prepaid.setCollectedBy(CollectedBy.BPP);
                prepaid.setInvoiceEvent(FulfillmentStatus.Created);
                prepaid.setId(PREPAID);
                if (!ObjectUtil.isVoid(company.getVirtualPaymentAddress())) {
                    prepaid.setParams(new Params() {{
                        setVirtualPaymentAddress(company.getVirtualPaymentAddress());
                        setBankAccountName(company.getAccountHolderName());
                        setMcc(company.getMerchantCategoryCode());
                        setCurrency(company.getFacilities().get(0).getCountry().getWorldCurrency().getCode());
                    }});
                }
                add(prepaid);
            }
            if (company.isPostDeliverySupported()) {
                Payment cod = new Payment();
                cod.setCollectedBy(CollectedBy.BPP);
                cod.setInvoiceEvent(FulfillmentStatus.Completed);
                cod.setId(POST_DELIVERY);
                if (!ObjectUtil.isVoid(company.getVirtualPaymentAddress())) {
                    cod.setParams(new Params() {{
                        setVirtualPaymentAddress(company.getVirtualPaymentAddress());
                        setBankAccountName(company.getAccountHolderName());
                        setMcc(company.getMerchantCategoryCode());
                        setCurrency(company.getFacilities().get(0).getCountry().getWorldCurrency().getCode());
                    }});
                }
                add(cod);
            }
        }};
    }
    
    private Locations getLocations(Company company) {
        Locations locations = new Locations();
        for (Facility facility : company.getFacilities()) {
            locations.add(toLocation(company, facility));
        }
        return locations;
    }
    
    private Location toLocation(Company company, Facility facility) {
        return new Location() {{
            setAddress(ECommerceAdaptor.this.getAddress(facility.getName(), facility));
            setPinCode(getAddress().getPinCode());
            setGps(new GeoCoordinate(facility));
            setCountry(new in.succinct.beckn.Country() {{
                setName(facility.getCountry().getName());
                setCode(facility.getCountry().getIsoCode());
            }});
            setCity(new City() {{
                setName(facility.getCity().getName());
                setCode(facility.getCity().getCode());
            }});
            setState(new State() {{
                setName(facility.getState().getName());
                setCode(facility.getState().getCode());
            }});
            setDescriptor(new Descriptor() {{
                setName(facility.getName());
            }});
            setId(BecknIdHelper.getBecknId(StringUtil.valueOf(facility.getId()), getSubscriber(), Entity.provider_location));
            if (company.isHomeDeliverySupported() && company.getMaxDistance() >= 0) {
                setCircle(new Circle() {{
                    setGps(new GeoCoordinate(facility));
                    setRadius(new Scalar() {{
                        setValue(Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter().valueOf(company.getMaxDistance()));
                        setUnit("km");
                    }});
                }});
            }
        }};
    }
    
    private Address getAddress(String name, com.venky.swf.plugins.collab.db.model.participants.admin.Address facility) {
        Address address = new Address();
        address.setState(facility.getState().getName());
        address.setName(name);
        address.setPinCode(facility.getPinCode().getPinCode());
        address.setCity(facility.getCity().getName());
        address.setState(facility.getState().getName());
        address.setCountry(facility.getCountry().getName());
        address._setAddressLines(facility.getAddressLine1(), facility.getAddressLine2(), facility.getAddressLine3(), facility.getAddressLine4());
        return address;
    }
    
    private Fulfillments getFulfillments(Company company) {
        return new Fulfillments() {{
            if (company.isHomeDeliverySupported()) {
                Fulfillment home_delivery = new Fulfillment();
                home_delivery.setId(RetailFulfillmentType.home_delivery.toString());
                home_delivery.setType(RetailFulfillmentType.home_delivery.toString());
                
                if (company.getMaxDistance() > 0) {
                    home_delivery.setTag("APPLICABILITY", "MAX_DISTANCE", company.getMaxDistance());
                    if (!ObjectUtil.isVoid(company.getNotesOnDeliveryCharges())) {
                        home_delivery.setTag("DELIVERY_CHARGES", "NOTES", company.getNotesOnDeliveryCharges());
                    }
                }
                add(home_delivery);
            }
            if (company.isStorePickupSupported()) {
                Fulfillment store_pickUp = new Fulfillment();
                store_pickUp.setId(RetailFulfillmentType.store_pickup.toString());
                store_pickUp.setType(RetailFulfillmentType.store_pickup.toString());
                add(store_pickUp);
            }
        }};
    }
    
    private Items getItems(Provider provider, ECommerceSDK helper) {
        Items items = new Items();
        Domain domain = null;
        
        for (Domain d : NetworkAdaptorFactory.getInstance().getAdaptor().getDomains()){
            if (ObjectUtil.equals(d.getDomainCategory(),DomainCategory.BUY_MOVABLE_GOODS)){
                domain = d;
                break;
            }
        }
        
        
        Store store  = helper.getStore();
        InventoryLocations inventoryLocations = helper.getInventoryLocations();
        InventoryItems inventoryItems = helper.getInventoryItems();
        Products products = helper.getProducts();
        
        Map<String, Double> taxRateMap = getTaxRateMap();
        
        for (Product product : products) {
            for (ProductVariant variant : product.getProductVariants()) {
                InventoryItem inventoryItem = inventoryItems.get(StringUtil.valueOf(variant.getInventoryItemId()));
                if (inventoryItem == null) {
                    continue;
                }
                if (!inventoryItem.isTracked() && inventoryLocations.get(variant.getInventoryItemId()).isEmpty()) {
                    inventoryLocations.get(variant.getInventoryItemId()).add(store.getPrimaryLocationId());
                }
                Domain productDomain = domain;
                inventoryLocations.get(variant.getInventoryItemId()).forEach(location_id -> {
                    String itemId = BecknIdHelper.getBecknId(variant.getId(), getSubscriber(), Entity.item);
                    
                    Item item = items.get(itemId);
                    if (item == null){
                        item = new Item();
                        item.setId(itemId);
                        Descriptor descriptor = new Descriptor();
                        item.setDescriptor(descriptor);
                        if (!ObjectUtil.isVoid(variant.getBarCode())) {
                            descriptor.setCode(variant.getBarCode());
                        } else {
                            descriptor.setCode(product.getTitle());
                        }
                        
                        if (ObjectUtil.equals(variant.getTitle(), "Default Title")) {
                            descriptor.setName(product.getTitle());
                        } else {
                            descriptor.setName(String.format("%s - ( %s )", product.getTitle(), variant.getTitle()));
                        }
                        
                        descriptor.setShortDesc(descriptor.getName());
                        descriptor.setLongDesc(descriptor.getName());
                        descriptor.setImages(new Images());
                        ProductImages productImages = product.getImages();
                        
                        if (variant.getImageId() > 0) {
                            ProductImage image = productImages.get(StringUtil.valueOf(variant.getImageId()));
                            if (image != null) {
                                descriptor.getImages().add(
                                        new Image(){{
                                            setUrl(image.getSrc());
                                            this.setSizeType(SizeType.custom);
                                            this.setHeight(image.getHeight());
                                            this.setWidth(image.getWidth());
                                        }}
                                );
                            }
                        } else if (!productImages.isEmpty()) {
                            for (Iterator<ProductImage> i = productImages.iterator(); i.hasNext() && descriptor.getImages().size() < 3; ) {
                                ProductImage image = i.next();
                                descriptor.getImages().add(
                                        new Image(){{
                                            setUrl(image.getSrc());
                                            this.setSizeType(SizeType.custom);
                                            this.setHeight(image.getHeight());
                                            this.setWidth(image.getWidth());
                                        }}
                                );
                            }
                        }
                        descriptor.setSymbol(descriptor.getImages().get(0).getUrl());
                        item.setCategoryIds(new BecknStrings());
                        item.setFulfillmentIds(new BecknStrings());
                        item.setPaymentIds(new BecknStrings());
                        item.setLocationIds(new BecknStrings());
                        for (Payment payment : provider.getPayments()) {
                            item.getPaymentIds().add(payment.getId()); //Only allow By BAP , ON_ORDER
                        }
                        for (Fulfillment fulfillment : provider.getFulfillments()) {
                            item.getFulfillmentIds().add(fulfillment.getId());
                        }
                        for (Location location : provider.getLocations()) {
                            item.getLocationIds().add(location.getId());
                        }
                        
                        if (inventoryItem.getCountryCodeOfOrigin() == null) {
                            //If no custom information is set, then assume store's country  as the origin of the product.
                            item.setCountryOfOrigin(Country.findByISO(provider.getLocations().get(0).getAddress().getCountry()).getIsoCode());
                        } else {
                            item.setCountryOfOrigin(Country.findByISO(inventoryItem.getCountryCodeOfOrigin()).getIsoCode());
                        }
                        item.setTag("DOMAIN","CATEGORY",DomainCategory.BUY_MOVABLE_GOODS.name());
                        if (!ObjectUtil.isVoid(productDomain)){
                            item.setTag ("DOMAIN","ID",productDomain.getId());
                        }
                        
                        items.add(item);
                        
                    }
                    
                    
                    
                    for (String tag : product.getTagSet()) {
                        String token = tag.toUpperCase();
                        item.getCategoryIds().add(token);
                        
                        if (provider.getCategories().get(token) == null){
                            Category category = provider.getObjectCreator().create(Category.class);
                            category.setId(token);
                            category.setDescriptor(provider.getObjectCreator().create(Descriptor.class));
                            Descriptor descriptor = category.getDescriptor();
                            descriptor.setName(token);
                            descriptor.setCode(token);
                            descriptor.setShortDesc(token);
                            descriptor.setLongDesc(token);
                            provider.getCategories().add(category);
                        }
                    }
                    item.setTag("general_attributes", "product_id", variant.getProductId());

                    Price price = new Price();
                    item.setPrice(price);
                    price.setMaximumValue(variant.getMrp());
                    price.setListedValue(variant.getPrice());
                    price.setValue(variant.getPrice());
                    price.setOfferedValue(variant.getPrice());
                    if (doubleTypeConverter.valueOf(price.getMaximumValue()) < doubleTypeConverter.valueOf(price.getValue())) {
                        price.setMaximumValue(price.getValue());
                    }
                    price.setCurrency(provider.getPayments().get(0).getParams().getCurrency());
                    item.setHsnCode(inventoryItem.getHarmonizedSystemCode());
                    item.setTaxRate(taxRateMap.get(item.getHsnCode()));
                    double taxRate = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter().valueOf(item.getTaxRate()) / 100.0;
                    Price unitTax = new Price();
                    unitTax.setCurrency(price.getCurrency());
                    double factor = store.isTaxesIncluded() ? (taxRate / (1 + taxRate)) : taxRate;
                    
                    unitTax.setValue(factor * item.getPrice().getValue());
                    unitTax.setListedValue(factor * item.getPrice().getListedValue());
                    unitTax.setOfferedValue(factor * item.getPrice().getOfferedValue());
                    item.setTax(unitTax);
                    
                });
            }
        }
        return items;
    }
    private static final TypeConverter<Double> doubleTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter();
    
    public Map<String, Double> getTaxRateMap() {
        return new Cache<>(0, 0) {
            @Override
            protected Double getValue(String taxClass) {
                if (!ObjectUtil.isVoid(taxClass)) {
                    AssetCode assetCode = AssetCode.findLike(taxClass, AssetCode.class);
                    if (assetCode != null) {
                        return assetCode.getReflector().getJdbcTypeHelper().getTypeRef(double.class).getTypeConverter().valueOf(assetCode.getGstPct());
                    }
                }
                return 0.0D;
            }
        };
    }
    
    //From shopify to beckn
    public Order convert(ECommerceSDK helper,ShopifyOrder eCommerceOrder){
        eCommerceOrder.loadMetaFields(helper);
        
        String transactionId = eCommerceOrder.getContext().getTransactionId();
        
        LocalOrderSynchronizer localOrderSynchronizer = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber());
        
        Order lastKnownOrder = localOrderSynchronizer.getLastKnownOrder(transactionId, true);
        if (eCommerceOrder.isDraft()) {
            localOrderSynchronizer.setLocalDraftOrderId(transactionId, eCommerceOrder.getId());
        }else {
            localOrderSynchronizer.setLocalOrderId(transactionId, eCommerceOrder.getId());
        }
        
        Order order = new Order();
        order.update(lastKnownOrder);
        
        Date updatedAt = eCommerceOrder.getUpdatedAt();
        
        localOrderSynchronizer.setFulfillmentStatusReachedAt(transactionId, eCommerceOrder.getFulfillmentStatus(), updatedAt, false);
        
        if (!localOrderSynchronizer.hasOrderReached(transactionId, eCommerceOrder.getStatus())) {
            order.setStatus(eCommerceOrder.getStatus());
        }
        localOrderSynchronizer.setStatusReachedAt(transactionId, eCommerceOrder.getStatus(), updatedAt, false);
        
        order.setUpdatedAt(DateUtils.max(updatedAt, order.getUpdatedAt()));
        
        
        if (order.getStatus() == Status.Cancelled) {
            if (order.getCancellation() == null) {
                order.setCancellation(new Cancellation());
                order.getCancellation().setCancelledBy(CancelledBy.PROVIDER);
                order.getCancellation().setSelectedReason(new Option());
                order.getCancellation().getSelectedReason().setDescriptor(new Descriptor());
                Descriptor descriptor = order.getCancellation().getSelectedReason().getDescriptor();
                descriptor.setCode(CancellationReasonCode.convertor.toString(eCommerceOrder.getCancellationReasonCode()));
                
                switch (eCommerceOrder.getCancellationReasonCode()) {
                    case ITEM_NOT_AVAILABLE:
                        descriptor.setLongDesc("One or more items in the Order not available");
                        break;
                    case BUYER_REFUSES_DELIVERY:
                        descriptor.setLongDesc("Buyer Refused delivery");
                        break;
                    default:
                        descriptor.setLongDesc("Unable to fulfill order!");
                        break;
                }
            }
        }
        
        
        setPayment(order, eCommerceOrder);
        
        Fulfillment fulfillment = order.getFulfillment();
        fulfillment.setFulfillmentStatus(eCommerceOrder.getFulfillmentStatus());
        
        if (order.getCreatedAt() == null) {
            order.setCreatedAt(eCommerceOrder.getCreatedAt());
        }
        
        return order;
    }
    
    
    private void setPayment(Order order, ShopifyOrder eCommerceOrder) {
        boolean sellerCollected = eCommerceOrder.isPaid();
        //Seller collected is transmitted to buyer as paid.
        boolean buyerPaid = order.isPaid();
        if (buyerPaid || !sellerCollected) {
            return;
        }
        
        Payment payment = order.getPayments().get(0);
        if (eCommerceOrder.getPaymentTerms() != null) {
            payment.getParams().setAmount(doubleTypeConverter.valueOf(eCommerceOrder.getPaymentTerms().getAmount()));
        } else {
            payment.getParams().setAmount(eCommerceOrder.getTotalPrice());
        }
        
        Invoices invoices = order.getInvoices();
        if (invoices.isEmpty()){
            LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).updateOrderStatus(order); // Ensure invoices are created.
        }
        Map<String,PaymentTransaction> paymentTransactionMap = new HashMap<>();
        for (Transaction t : eCommerceOrder.getTransactions()) {
            if (ObjectUtil.equals(t.getStatus(),"success")) {
                if (ObjectUtil.equals("capture", t.getKind()) || ObjectUtil.equals("sale", t.getKind())) {
                    paymentTransactionMap.put(t.getId(),new PaymentTransaction() {{
                        setAmount(t.getAmount());
                        setDate(t.getCreatedAt());
                        setTransactionId(t.getId());
                        setPaymentStatus(PaymentStatus.PAID);
                    }});
                }
            }
        }
        List<Invoice> unpaidInvoices = new ArrayList<>();
        for (Invoice invoice : invoices) {
            if (invoice.getUnpaidAmount().intValue() > 0 ){
                unpaidInvoices.add(invoice);
            }
            for (PaymentTransaction paymentTransaction : invoice.getPaymentTransactions()) {
                paymentTransactionMap.remove(paymentTransaction.getTransactionId());
            }
        }
        if (unpaidInvoices.size() == 1){
            for (PaymentTransaction value : paymentTransactionMap.values()) {
                unpaidInvoices.get(0).getPaymentTransactions().add(value);
            }
        }else {
            throw new RuntimeException("Cannot have multiple unpaid invoices!");
        }
    }
    
    
    public ShopifyOrder findShopifyOrder(ECommerceSDK helper, Order order){
        String shopifyOrderId = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(order);
        String draftOrderId = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalDraftOrderId(order);
        if (shopifyOrderId != null){
            return helper.findShopifyOrder(shopifyOrderId, false);
        }else if (draftOrderId != null){
            return helper.findShopifyOrder(draftOrderId,true);
        }
        return null;
    }
    //From beckn to shopify
    public ShopifyOrder save(Request request, boolean create){
        ShopifyOrder shopifyOrder = new ShopifyOrder();
        Order bo = request.getMessage().getOrder();
        Provider provider = bo.getProvider();
        
        User user = getUser(request);
        ECommerceSDK helper = new ECommerceSDK(user.getCredentials(true,getCredentialAttributes()));
        
        Location storeLocation = bo.getProviderLocation();
        
        shopifyOrder.setId(LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).getLocalOrderId(request.getContext().getTransactionId()));
        
        shopifyOrder.setCurrency(provider.getPayments().get(0).getParams().getCurrency());
        shopifyOrder.setSourceName("beckn");
        shopifyOrder.setName("beckn-" + request.getContext().getTransactionId());
        shopifyOrder.setNoteAttributes(new NoteAttributes());
        
        for (String key : new String[]{"bap_id", "bap_uri", "domain", "transaction_id", "city", "country", "core_version", "ttl"}) {
            TagGroup meta = new TagGroup();
            meta.setName(String.format("context.%s", key));
            meta.setValue(request.getContext().get(key));
            shopifyOrder.getNoteAttributes().add(meta);
        }
        
        if (!ObjectUtil.isVoid(shopifyOrder.getId()) && create) {
            helper.delete(shopifyOrder);
        }
        
        setShipping(bo, shopifyOrder);
        
        setBilling(bo.getBilling(), shopifyOrder);
        Bucket totalPrice = new Bucket();
        Bucket tax = new Bucket();
        shopifyOrder.setLocationId(helper.getStore().getPrimaryLocationId());
        shopifyOrder.setTaxesIncluded(isTaxIncludedInPrice());
        
        if (bo.getItems() != null) {
            bo.getItems().forEach(boItem -> {
                
                LineItem lineItem = addItem(shopifyOrder, boItem);
                lineItem.setTaxLines(new TaxLines());
                
                double linePrice = boItem.getPrice().getValue() * lineItem.getQuantity();
                double taxRate = doubleTypeConverter.valueOf(boItem.getTaxRate()) / 100.0;
                double lineTax = linePrice * (isTaxIncludedInPrice() ? taxRate / (1.0 + taxRate) : taxRate);
                totalPrice.increment(linePrice);
                tax.increment(lineTax);
                
                String[] taxHeads = new String[]{"IGST"};
                if (ObjectUtil.equals(storeLocation.getState().getName(), bo.getBilling().getAddress().getState())) {
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
            });
        }
        shopifyOrder.setTotalTax(tax.doubleValue());
        LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(getSubscriber()).sync(request.getContext().getTransactionId(), bo);
        if (!shopifyOrder.getLineItems().isEmpty()) {
            saveDraftOrder(helper,shopifyOrder);
        }
        return  shopifyOrder;
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
        if (inspectQuantity.containsKey("count")) {
            lineItem.setQuantity(item.getQuantity().getCount());
        } else if (inspectQuantity.containsKey("selected")) {
            lineItem.setQuantity(item.getItemQuantity().getSelected().getCount());
        }

        lineItem.setProductId(doubleTypeConverter.valueOf(item.getTags().get("product_id")).longValue());
        lineItem.setRequiresShipping(true);
        lineItem.setTaxable(doubleTypeConverter.valueOf(item.getTaxRate()) > 0);
        line_items.add(lineItem);
        return lineItem;
    }
    
    private boolean isTaxIncludedInPrice() {
        return true;
    }
   private void saveDraftOrder(ECommerceSDK helper,ShopifyOrder draftOrder) {
        JSONObject dro = new JSONObject();
        dro.put("draft_order", draftOrder.getInner());

        JSONObject outOrder = helper.post("/draft_orders.json", dro);
        ShopifyOrder oDraftOrder = new ShopifyOrder((JSONObject) outOrder.get("draft_order"));
        draftOrder.setPayload(oDraftOrder.getInner().toString());
        draftOrder.setDraft(true);

    }
    
    public void setShipping(Order bo, ShopifyOrder target) {
        Fulfillment source = bo.getFulfillment();
        
        if (source == null) {
            return;
        }
        ShopifyOrder.Address shipping = new ShopifyOrder.Address();
        target.setShippingAddress(shipping);
        
        in.succinct.beckn.User user = source.getCustomer();
        
        Address address = source._getEnd().getLocation().getAddress();
        if (address != null) {
            if (user == null ) {
                user = new in.succinct.beckn.User();
            }
            if (user.getPerson() == null) {
                user.setPerson(new Person());
            }
            if (ObjectUtil.isVoid(user.getPerson().getName())) {
                user.getPerson().setName(address.getName());
            }
        }
        
        if (user != null && !ObjectUtil.isVoid(user.getPerson().getName())) {
            String[] parts = user.getPerson().getName().split(" ");
            shipping.setName(user.getPerson().getName());
            shipping.setFirstName(parts[0]);
            shipping.setLastName(user.getPerson().getName().substring(parts[0].length()));
        }
        
        
        Contact contact = source._getEnd().getContact();
        GeoCoordinate gps = source._getEnd().getLocation().getGps();
        
        if (address != null) {
            if (address.getCountry() == null) {
                address.setCountry(source._getStart().getLocation().getCountry().getName());
            }
            if (address.getState() == null ){
                address.setState(source._getStart().getLocation().getState().getName());
            }
            if (address.getCity() == null){
                address.setCity(source._getStart().getLocation().getCity().getName());
            }
            Country country = Country.findByName(address.getCountry());
            com.venky.swf.plugins.collab.db.model.config.State state = com.venky.swf.plugins.collab.db.model.config.State.findByCountryAndName(country.getId(), address.getState());
            
            String[] lines = address._getAddressLines(2);
            shipping.setAddress1(lines[0]);
            shipping.setAddress2(lines[1]);
            
            shipping.setCity(address.getCity());
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
        
        
        if (bo.getBilling() == null) {
            bo.setBilling(new Billing());
        }
        if (bo.getBilling().getAddress() == null) {
            bo.getBilling().setAddress(new Address());
        }
        bo.getBilling().getAddress().update(address,false);
        if (bo.getBilling().getName() == null) {
            bo.getBilling().setName(bo.getBilling().getAddress().getName());
        }
        
    }
    
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
            com.venky.swf.plugins.collab.db.model.config.State state = com.venky.swf.plugins.collab.db.model.config.State.findByCountryAndName(country.getId(), source.getAddress().getState());
            com.venky.swf.plugins.collab.db.model.config.City city =  com.venky.swf.plugins.collab.db.model.config.City.findByStateAndName(state.getId(), source.getAddress().getCity());

            billing.setCity(city.getName());
            billing.setProvince(city.getState().getName());
            billing.setProvinceCode(city.getState().getCode());
            billing.setCountryCode(city.getState().getCountry().getIsoCode2());
            billing.setCountry(country.getName());
            billing.setZip(source.getAddress().getAreaCode());
        }


        billing.setPhone(source.getPhone());

    }


}
