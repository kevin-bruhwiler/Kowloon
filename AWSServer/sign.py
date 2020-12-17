from Crypto.PublicKey import RSA
from Crypto import Random
from Crypto.Hash import SHA256
from Crypto.Signature import pkcs1_15
import base64


def rsakeys():
    length = 2048
    private_key = RSA.generate(length, Random.new().read)
    public_key = private_key.publickey()
    return private_key, public_key


def sign(private_key, data):
    return pkcs1_15.new(private_key).sign(SHA256.new(data))


def verify(public_key, data, signature):
    try:
        pkcs1_15.new(public_key).verify(SHA256.new(data), signature)
        return True
    except (ValueError, TypeError):
        return False


def generate_keys():
    private_key, public_key = rsakeys()

    with open("./private.pem", "wb") as prv_file:
        prv_file.write(private_key.exportKey())

    with open("../public.pem", "wb") as pub_file:
        pub_file.write(public_key.exportKey())


def load_saved_keys():
    with open("./private.pem", "rb") as prv_file:
        private_key = RSA.importKey(prv_file.read())

    with open("./public.pem", "rb") as pub_file:
        public_key = RSA.importKey(pub_file.read())

    return private_key, public_key


if __name__ == "__main__":
    d = b"hello there"
    generate_keys()
    prv_key, pub_key = load_saved_keys()
    sig = sign(prv_key, d)
    print(sig, "\n", pub_key.exportKey())
    print(verify(pub_key, d, sig))
