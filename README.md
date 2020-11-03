# Studio Inertia Application Note

## What am I?
This repository contains the Python cloud function for receiving, authenticating and modeling temperature data from a DT Studio Data Connector. It has been written to work as a Google Cloud Function, but can with minor editing work on any of the major cloud hosting services. It is developed to serve as an example implementation on how to perform custom data manipulation in DT Studio, explained in [this application note](https://support.disruptive-technologies.com/hc/en-us/articles/360017283539).

## Before You Deploy
Environment variables must be set. For this, create a *.env.yaml* file with the following content.
```yaml
SERVICE_ACCOUNT_KEY_ID: ___
SERVICE_ACCOUNT_SERCRET: ___
SERVICE_ACCOUNT_EMAIL: ___
DT_SIGNATURE_SECRET: ___
AUTH_ENDPOINT: https://identity.disruptive-technologies.com/oauth2/token
API_URL_BASE: https://api.disruptive-technologies.com/v2
EMU_URL_BASE: https://emulator.disruptive-technologies.com/v2
```
The service account key, secret, and email are those create by a DT Studio Service account. This is used for authentication when interfacing with the API. The signature secret should be a strong and unique password, also used when creating a new Data Connector.

## Deploy
Deployment is easiest through the use of the Google Cloud CLI. After changing the capitalized arguments below, a single call is enough to push a new verison of the function.
```bash
gcloud functions deploy YOUR_FUNCTION_NAME \
    --entry-point main \
    --runtime python37 \
    --trigger-http \
    --allow-unauthenticated \
    --timeout 30s \
    --ignore-file .gcloudignore \
    --project YOUR_GCLOUD_PROJECT \
    --region YOUR_REGION \
    --env-vars-file .env.yaml
```

## Local Development
To develop locally, install the Python developer requirements using the provided file.
```python
pip install -r requirements_dev.txt
```
Set the necessary local environment variables, as shown in the .yaml file above, with your method of choice. Then, using the function-framwork developed by Google, test the function by serving it to LocalHost.
```bash
functions-framework --source main.py --target=main --debug
```

