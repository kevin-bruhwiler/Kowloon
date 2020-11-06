import unittest
from start import get_app
from sign import rsakeys
import json


class MiningTest(unittest.TestCase):
    def setUp(self):
        self.client1 = get_app().test_client()
        self.client2 = get_app().test_client()

    def test_different_lengths(self):
        public_key1 = "key1"
        public_key2 = "key2"

        for block in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (1, 0, 0), (2, 0, 0)]:
            response = self.client1.get('/mine', data=json.dumps({"index": block, "signature": public_key1}),
                                        content_type='application/json')
            self.assertEqual(response.status_code, 200)

        for block in [(0, 0, 0), (0, 0, 1), (1, 0, 0), (2, 0, 0)]:
            response = self.client2.get('/mine', data=json.dumps({"index": block, "signature": public_key2}),
                                        content_type='application/json')
            self.assertEqual(response.status_code, 200)

        grid1 = dict(self.client1.get('/grid').get_json().get('grid'))
        grid2 = dict(self.client2.get('/grid').get_json().get('grid'))

        grid2_auth = self.client1.get('/grid/compare', data=json.dumps({"grid": grid2}),
                                      content_type='application/json')
        grid1_auth = self.client2.get('/grid/compare', data=json.dumps({"grid": grid1}),
                                      content_type='application/json')

        self.assertEqual(grid1_auth.get_json().get("auth"), True)
        self.assertEqual(grid2_auth.get_json().get("auth"), False)
