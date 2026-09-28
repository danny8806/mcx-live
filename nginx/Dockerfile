# nginx single ingress for the LIVE gateway.
# Build context is repo root (.), so COPY paths are relative to that.
FROM nginx:1.27-alpine
COPY nginx/nginx.conf /etc/nginx/nginx.conf
COPY nginx/conf.d/default.conf /etc/nginx/conf.d/default.conf
COPY nginx/proxy_params /etc/nginx/proxy_params
COPY nginx/ws_params /etc/nginx/ws_params
EXPOSE 80 443
STOPSIGNAL SIGQUIT