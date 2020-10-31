import unittest
from start import app
from sign import rsakeys
import json


class MiningTest(unittest.TestCase):
    def setUp(self):
        self.client1 = app.test_client()
        self.client2 = app.test_client()

    def test_different_lengths(self):
        private_key, public_key = rsakeys()
        public_key = public_key.exportKey().decode("utf-8").strip("-----BEGIN PUBLIC KEY-----\n").strip("\n-----END PUBLIC KEY-----")

        for block in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (1, 0, 0), (2, 0, 0)]:
            response = self.client1.get('/mine', data=json.dumps({"index": block, "signature": public_key}),
                                        content_type='application/json')
            self.assertEqual(response.status_code, 200)

        for block in [(0, 0, 0), (0, 0, 1), (1, 0, 0), (2, 0, 0)]:
            response = self.client2.get('/mine', data=json.dumps({"index": block, "signature": public_key}),
                                        content_type='application/json')
            self.assertEqual(response.status_code, 200)
