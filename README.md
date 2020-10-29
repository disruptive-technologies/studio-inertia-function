# Studio Inertia Application Note

## Deploy
```
gcloud functions deploy studio-inertia-an --entry-point dataconnector_endpoint --runtime python37 --trigger-http --allow-unauthenticated --timeout 30s --ignore-file .gcloudignore --project YOUR_GCLOUD_PROJECT --region YOUR_REGION
```
