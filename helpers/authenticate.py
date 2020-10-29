# packages
import jwt
import time
import hashlib
import requests


def project_validate(request, header, secret):
    """
    Validate request content.
    Checks secret and checksum.

    Parameters
    ----------
    request : dictionary
        HTTP POST request received.
    header : str
        Custom DT JWT header in request.
    secret : str
        Password used to sign request content.

    Returns
    -------
    status : tuple
        Tuple with 2 cells containing status text [0] and status code [1].

    """

    # check for signature environment variable
    if secret == None:
        return ('missing secret', 400)

    # check for dt header in request
    if header not in request.headers:
        return ('missing header', 400)

    # verify secret against environment variable
    token = request.headers[header]
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
    except:
        return ('signature error', 400)

    # verify body checksum
    m = hashlib.sha1()
    m.update(request.get_data())
    checksum = m.digest().hex()

    if payload["checksum"] != checksum:
        return ('checksum mismatch', 400)

    # success
    return ('OK', 200)


def authenticate_service_account(email, key_id, secret, auth_endpoint):
    """
    Perform OAuth2 authentication flow for DT Authentication.
    Uses service accounts for access control.
    Uses JWT as the medium for the exchange.
    https://support.disruptive-technologies.com/hc/en-us/articles/360011534099-Authentication

    Parameters
    ----------
    email : str
        Service account email.
    key_id : str
        Service account public key.
    secret : str
        Password used to sign request content.
    auth_endpoint : str
        Endpoint for authentication request.

    Returns
    -------
    access_token : str
        Token used to authenticate future POST requests.

    """

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

