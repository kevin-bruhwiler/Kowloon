import json
from uuid import uuid4
from flask import Flask, jsonify, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from sign import load_saved_keys, sign

from blockgrid import Blockgrid


def get_app():
    app = Flask(__name__)

    limiter = Limiter(
        app,
        key_func=get_remote_address,
        default_limits=["200 per day", "50 per hour"]
    )

    node_identifier = str(uuid4()).replace('-', '')
    blockgrid = Blockgrid()

    # if os.path.isfile("./blockgrid.pkl"):
    #    blockgrid.load("blockgrid.pkl")

    @app.route('/mine', methods=['GET'])
    @limiter.limit("20 per hour")
    def mine():
        # We run the proof of work algorithm to get the next proof...
        values = request.get_json()

        index = tuple(values["index"])

        if index not in blockgrid.grid:
            return 'Previous block has not been mined', 400

        if blockgrid.grid[index]["owner"] is not None:
            return 'Block has already been mined', 400

        block = blockgrid.grid[index].copy()
        block["owner"] = values["signature"]
        last_proof = blockgrid.hash_without_proof(block)
        proof = blockgrid.proof_of_work(last_proof, index)

        # Forge the new Block by adding it to the chain
        blockgrid.sign_block(index, proof, values["signature"])

        response = {
            'message': "New Block Forged",
            'index': blockgrid.grid[index]['index'],
            'owner': blockgrid.grid[index]['owner'],
            'data': blockgrid.grid[index]['data'],
            'proof': blockgrid.grid[index]['proof'],
            'previous_hash': blockgrid.grid[index]['previous_hash'],
        }

        return jsonify(response), 200

    @app.route('/transactions/new', methods=['POST'])
    def new_transaction():
        values = request.get_json()

        # Check that the required fields are in the POST'ed data
        required = ['index', 'data', 'signature']
        if not all(k in values for k in required):
            return 'Missing values', 400

        # Create a new Transaction
        index = blockgrid.new_transaction(tuple(values['index']), values['data'], values['signature'])

        response = {'message': f'Transaction will be added to Block {index}'}

        # blockgrid.save("blockgrid.pkl")

        return jsonify(response), 200

    @app.route('/transactions/new/unsigned', methods=['POST'])
    def new_unsigned_transaction():
        values = request.get_json()

        for k, v in values.items():
            if k == "delete":
                for d in blockgrid.grid[(0, 0, 0)]["data"]:
                    keys_to_remove = []
                    key_data = json.loads(d["data"])
                    for k2, v2 in key_data.items():
                        if k2 in v and v2["filepath"] == v[k2]:
                            keys_to_remove.append(k2)
                    for key in keys_to_remove:
                        del key_data[key]
                    d["data"] = json.dumps(key_data)
            else:
                blockgrid.asset_bundles.add((v["filepath"], v["bundle"]))
                del v["bundle"]

        final = {"index": (0, 0, 0), "data": json.dumps(values)}
        private_key, _ = load_saved_keys()
        final["signature"] = sign(private_key, final["data"].encode('utf-8')).decode('latin-1')

        # Create a new Transaction
        index = blockgrid.new_transaction(tuple(final['index']), final['data'], final['signature'])

        response = {'message': f'Transaction will be added to Block {index}'}

        return jsonify(response), 200

    # This has to be a POST type because of unity HTTP stupidity, really should be GET
    @app.route('/grid/index', methods=['POST'])
    @limiter.limit("50 per hour")
    def data_at_index():
        values = request.get_json()

        required = ['index']
        if not all(k in values for k in required):
            return 'Missing values', 400

        index = tuple(int(x / 500) for x in values['index'])
        response = {
            'block': blockgrid.grid[index],
            'bundles': list(blockgrid.asset_bundles),
            'type': "grid/index"
        }
        return jsonify(response), 200

    @app.route('/grid', methods=['GET'])
    @limiter.limit("10 per hour")
    def full_grid():
        response = {
            'grid': {":".join(map(str, k)): v for k, v in blockgrid.grid.items()},
            'length': len(blockgrid.grid),
        }
        return jsonify(response), 200

    @app.route('/grid/compare', methods=['GET'])
    @limiter.limit("10 per hour")
    def compare_grids():
        values = request.get_json()

        other_grid = {tuple(map(int, k.split(":"))): v for k, v in values.get('grid').items()}

        response = {
            'auth': blockgrid.compare_grids(other_grid),
        }
        return jsonify(response), 200

    @app.route('/grid/replace', methods=['PUT'])
    @limiter.limit("10 per hour")
    def replace_grid():
        values = request.get_json()

        other_grid = {tuple(map(int, k.split(":"))): v for k, v in values.get('grid').items()}
        blockgrid.replace_grid(other_grid)

        response = {
            'message': "grid has been replaced",
        }
        return jsonify(response), 200

    @app.route('/grid/update', methods=['GET'])
    @limiter.limit("10 per hour")
    def update_grids():
        values = request.get_json()

        grid1 = {tuple(map(int, k.split(":"))): v for k, v in values.get('shorter_grid').items()}
        grid2 = {tuple(map(int, k.split(":"))): v for k, v in values.get('longer_grid').items()}

        response = {
            'grid': {":".join(map(str, k)): v for k, v in blockgrid.update_grid(grid1, grid2).items()},
        }
        return jsonify(response), 200

    @app.route('/nodes/register', methods=['POST'])
    @limiter.limit("10 per hour")
    def register_nodes():
        values = request.get_json()

        nodes = values.get('nodes')
        if nodes is None:
            return "Error: Please supply a valid list of nodes", 400

        for node in nodes:
            blockgrid.register_node(node)

        response = {
            'message': 'New nodes have been added',
            'total_nodes': list(blockgrid.nodes),
        }
        return jsonify(response), 200

    @app.route('/nodes/resolve', methods=['GET'])
    @limiter.limit("10 per hour")
    def consensus():
        replaced = blockgrid.resolve_conflicts()

        if replaced:
            response = {
                'message': 'Our chain was replaced',
                'new_chain': blockgrid.grid
            }
        else:
            response = {
                'message': 'Our chain is authoritative',
                'chain': blockgrid.grid
            }

        return jsonify(response), 200

    return app


if __name__ == '__main__':
    app = get_app()
    app.run(host='0.0.0.0', port=5000)
