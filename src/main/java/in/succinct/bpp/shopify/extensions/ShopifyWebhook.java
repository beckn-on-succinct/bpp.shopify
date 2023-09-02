package in.succinct.bpp.shopify.extensions;

import com.venky.core.security.Crypt;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.swf.db.Database;
import com.venky.swf.path.Path;
import com.venky.swf.path._IPath;
import com.venky.swf.plugins.background.core.DbTask;
import com.venky.swf.plugins.background.core.TaskManager;
import com.venky.swf.routing.Config;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public abstract class ShopifyWebhook implements Extension {

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
            _hook(eCommerceAdaptor, networkAdaptor,path);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    public void _hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor,Path path) throws Exception {
        String payload = StringUtil.read(path.getInputStream());
        //Validate auth headers from path.getHeader
        String sign = path.getHeader("X-Shopify-Hmac-SHA256");

        if (Config.instance().isDevelopmentEnvironment()) {
            StringBuilder fakeCurlHeader = new StringBuilder();
            path.getHeaders().forEach((k,v)->{
                fakeCurlHeader.append(String.format(" -H \"%s:%s\"",k,v));
            });
            Config.instance().getLogger(getClass().getName()).info(String.format("curl %s \"%s\" -d '%s'",  fakeCurlHeader, path.getRequest().getRequestURL(),payload));
        }


        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(
                eCommerceAdaptor.getConfiguration().get("in.succinct.bpp.shopify.secret").getBytes(StandardCharsets.UTF_8),
                "HmacSHA256"));
        byte[] hmacbytes = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
        if (!ObjectUtil.equals(Crypt.getInstance().toBase64(hmacbytes), sign)) {
            throw new RuntimeException("Webhook - Signature failed!!");
        }

        Map<String,Object> attributes = Database.getInstance().getCurrentTransaction().getAttributes();
        Map<String,Object> context = Database.getInstance().getContext();

        TaskManager.instance().executeAsync((DbTask)()->{
            Database.getInstance().getCurrentTransaction().setAttributes(attributes);
            if (context != null) {
                context.remove(_IPath.class.getName());
                Database.getInstance().setContext(context);
            }

            hook(eCommerceAdaptor,networkAdaptor,path,payload);
        },false);


    }

    public abstract void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor,Path path,String payload);
}
