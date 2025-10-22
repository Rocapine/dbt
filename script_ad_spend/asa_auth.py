import os
import datetime as dt
from authlib.jose import jwt
from Crypto.PublicKey import ECC


AUDIENCE = "https://appleid.apple.com"
ALG = "ES256"


def build_client_secret(client_id: str, team_id: str, key_id: str, private_key_pem: str, *, expiration_seconds: int = 86400 * 180) -> str:
    """Build an Apple Search Ads OAuth client_secret JWT using the provided key.

    The private key MUST be the .p8 key issued by Apple for the given key_id and team_id.
    """
    issued_at_timestamp = int(dt.datetime.now(dt.timezone.utc).timestamp())
    expiration_timestamp = issued_at_timestamp + expiration_seconds

    headers = {
        "alg": ALG,
        "kid": key_id,
    }
    payload = {
        "sub": client_id,
        "aud": AUDIENCE,
        "iat": issued_at_timestamp,
        "exp": expiration_timestamp,
        "iss": team_id,
    }

    private_key = ECC.import_key(private_key_pem)
    client_secret = jwt.encode(header=headers, payload=payload, key=private_key.export_key(format='PEM')).decode("UTF-8")
    return client_secret


if __name__ == "__main__":
    client_id = "SEARCHADS.249553e6-77cd-403e-92dc-2e9e3d4e7467"
    team_id = "SEARCHADS.249553e6-77cd-403e-92dc-2e9e3d4e7467"
    key_id = "b0366540-34e1-4639-845c-ef5928dfdf51"
    private_key_pem = os.getenv("ASA_PRIVATE_KEY_PEM", "").strip()
    if not all([client_id, team_id, key_id, private_key_pem]):
        raise SystemExit("Missing ASA_CLIENT_ID/ASA_TEAM_ID/ASA_KEY_ID/ASA_PRIVATE_KEY_PEM env vars")
    cs = build_client_secret(client_id, team_id, key_id, private_key_pem)
    with open("client_secret.txt", "w") as output:
        output.write(cs)