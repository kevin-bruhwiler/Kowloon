import unittest
from start import get_app
from sign import rsakeys
import json


class MiningTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_different_lengths(self):
        client1 = get_app().test_client()
        client2 = get_app().test_client()
        public_key1 = "key1"
        public_key2 = "key2"

        for block in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (1, 0, 0), (2, 0, 0)]:
            response = client1.get('/mine', data=json.dumps({"index": block, "signature": public_key1}),
                                   content_type='application/json')
            self.assertEqual(response.status_code, 200)

        for block in [(0, 0, 0), (0, 0, 1), (1, 0, 0), (2, 0, 0)]:
            response = client2.get('/mine', data=json.dumps({"index": block, "signature": public_key2}),
                                   content_type='application/json')
            self.assertEqual(response.status_code, 200)

        grid1 = dict(client1.get('/grid').get_json().get('grid'))
        grid2 = dict(client2.get('/grid').get_json().get('grid'))

        grid2_auth = client1.get('/grid/compare', data=json.dumps({"grid": grid2}),
                                 content_type='application/json')
        grid1_auth = client2.get('/grid/compare', data=json.dumps({"grid": grid1}),
                                 content_type='application/json')

        self.assertEqual(grid1_auth.get_json().get("auth"), True)
        self.assertEqual(grid2_auth.get_json().get("auth"), False)

    def test_same_length_recent_update(self):
        client1 = get_app().test_client()
        client2 = get_app().test_client()
        public_key = "key"

        for block in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (1, 0, 0), (2, 0, 0)]:
            response = client1.get('/mine', data=json.dumps({"index": block, "signature": public_key}),
                                   content_type='application/json')
            self.assertEqual(response.status_code, 200)

        client2.put('/grid/replace', data=json.dumps({"grid": dict(client1.get('/grid').get_json().get('grid'))}),
                    content_type='application/json')

        for block in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (1, 0, 0), (2, 0, 0)]:
            client2.post('/transactions/new', data=json.dumps({'index': block, 'data': "test", 'signature': public_key})
                         , content_type='application/json')
            self.assertEqual(response.status_code, 200)

        grid1 = dict(client1.get('/grid').get_json().get('grid'))
        grid2 = dict(client2.get('/grid').get_json().get('grid'))
        new_grid = client1.get('/grid/update', data=json.dumps({"shorter_grid": grid1, "longer_grid": grid2}),
                               content_type='application/json')

        self.assertDictEqual(grid2, new_grid.get_json().get("grid"))

    def test_different_length_update(self):
        client1 = get_app().test_client()
        client2 = get_app().test_client()
        public_key = "key"

        for block in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (1, 0, 0), (2, 0, 0)]:
            response = client1.get('/mine', data=json.dumps({"index": block, "signature": public_key}),
                                   content_type='application/json')
            self.assertEqual(response.status_code, 200)

        client2.put('/grid/replace', data=json.dumps({"grid": dict(client1.get('/grid').get_json().get('grid'))}),
                    content_type='application/json')

        for block in [(0, 0, 2), (0, 2, 0)]:
            response = client1.get('/mine', data=json.dumps({"index": block, "signature": public_key}),
                                   content_type='application/json')
            self.assertEqual(response.status_code, 200)

        grid1 = dict(client1.get('/grid').get_json().get('grid'))
        grid2 = dict(client2.get('/grid').get_json().get('grid'))
        new_grid = client2.get('/grid/update', data=json.dumps({"shorter_grid": grid1, "longer_grid": grid2}),
                               content_type='application/json')

        self.assertDictEqual(grid1, new_grid.get_json().get("grid"))
