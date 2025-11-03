package in.succinct.bpp.shopify.extensions;

import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import in.succinct.bpp.core.db.model.AdaptorCredential;
import org.json.simple.JSONObject;

import java.util.HashMap;

public class CredentialsTemplate implements Extension {
    static {
        Registry.instance().registerExtension("bpp.shell.fill.credentials",new CredentialsTemplate());
    }
    
    @Override
    public void invoke(Object... context) {
        JSONObject template = (JSONObject) context[0];
        AdaptorCredential adaptorCredential = (AdaptorCredential)context[1];
        if (ObjectUtil.equals(adaptorCredential.getAdaptorName(),"shopify")) {
            template.putAll(new HashMap<String, String>() {{
                put("X-Shopify-Access-Token","");
                put("X-Store-Url","");
            }});
        }
    }
}
