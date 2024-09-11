

GOOGLE CLOUD

gcloud functions delete integration --gen2 --region us-central1
gcloud functions deploy integration --gen2 --trigger-http --runtime="nodejs20" --entry-point=integration --region=us-central1 --allow-unauthenticated

API GATEWAY

Create an API:

project=cald-ads-qa
api=alerts-api
openapi-spec=openapi2-functions.yaml
backend-auth-service-account=cald-ads-qa@appspot.gserviceaccount.com
location=us-central1

CREATE AN API:

gcloud api-gateway apis create alerts-api --project=cald-ads-qa


CREATE AN API CONFIG:

gcloud api-gateway api-configs create alerts-config --api=alerts-api --openapi-spec=openapi-functions.yaml --project=cald-ads-qa --backend-auth-service-account=cald-ads-qa@appspot.gserviceaccount.com

UPDATE 
gcloud api-gateway api-configs create alerts-configv9 --api=alerts-api --openapi-spec=openapi-functions.yaml --project=cald-ads-qa --backend-auth-service-account=cald-ads-qa@appspot.gserviceaccount.com

ENABLE API:

gcloud api-gateway apis describe alerts-api --project=cald-ads-qa
gcloud services enable alerts-api-3bemsjbxbpdnr.apigateway.cald-ads-qa.cloud.goog

CREATE A GATEWAY:

gcloud api-gateway gateways create alerts-api --api=alerts-api --api-config=alerts-config --location=us-central1 --project=cald-ads-qa

UPDATE:
gcloud api-gateway gateways update alerts-api --api=alerts-api --api-config=alerts-configv9 --location=us-central1 --project=cald-ads-qa