from Crypto.PublicKey import RSA
from Crypto import Random
import base64


def rsakeys():
    length = 4096
    private_key = RSA.generate(length, Random.new().read)
    public_key = private_key.publickey()
    return private_key, public_key


def sign(private_key, data):
    return base64.b64encode(str((private_key.sign(data, ''))[0]).encode())


def verify(public_key, data, signature):
    return public_key.verify(data, (int(base64.b64decode(signature)),))


def generate_keys():
    private_key, public_key = rsakeys()

    with open("private.pem", "wb") as prv_file:
        prv_file.write(private_key.exportKey())

    with open("public.pem", "wb") as pub_file:
        pub_file.write(public_key.exportKey())


def load_saved_keys():
    with open("private.pem", "rb") as prv_file:
        private_key = RSA.importKey(prv_file.read())

    with open("public.pem", "rb") as pub_file:
        public_key = RSA.importKey(pub_file.read())

    return private_key, public_key


if __name__ == "__main__":
    d = b"hello there"
    prv_key, pub_key = load_saved_keys()
    sig = sign(prv_key, d)
    print(sig, "\n", pub_key.exportKey())
