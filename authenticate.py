# packages
import time
import jwt
import requests


def authenticate_service_account(email, key_id, secret, auth_endpoint):
    # create jwt
    headers = {
        "alg": "HS256",
        "kid": key_id
    }
    payload = {
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,
        "aud": auth_endpoint,
        "iss": email
    }
    encoded_jwt = jwt.encode(payload=payload,
                             key=secret,
                             algorithm='HS256',
                             headers=headers)

    parameters = {
        'assertion': encoded_jwt,
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer'
    }

    response = requests.post(auth_endpoint, data=parameters).json()

    try:
        access_token = 'Bearer ' + response['access_token']
    except KeyError:
        return None

    return access_token
