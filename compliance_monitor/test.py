import os
for k in ["HTTP_PROXY","HTTPS_PROXY","http_proxy","https_proxy","NO_PROXY","no_proxy"]:
    print(k, "=", os.environ.get(k))