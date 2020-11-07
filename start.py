from uuid import uuid4
from flask import Flask, jsonify, request

import os

from blockgrid import Blockgrid


def get_app():
    app = Flask(__name__)
    node_identifier = str(uuid4()).replace('-', '')
    blockgrid = Blockgrid()

    # if os.path.isfile("./blockgrid.pkl"):
    #    blockgrid.load("blockgrid.pkl")

    @app.route('/mine', methods=['GET'])
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

    @app.route('/grid', methods=['GET'])
    def full_grid():
        response = {
            'grid': {":".join(map(str, k)): v for k, v in blockgrid.grid.items()},
            'length': len(blockgrid.grid),
        }
        return jsonify(response), 200

    @app.route('/grid/compare', methods=['GET'])
    def compare_grids():
        values = request.get_json()

        other_grid = {tuple(map(int, k.split(":"))): v for k, v in values.get('grid').items()}

        response = {
            'auth': blockgrid.compare_grids(other_grid),
        }
        return jsonify(response), 200

    @app.route('/grid/replace', methods=['PUT'])
    def replace_grid():
        values = request.get_json()

        other_grid = {tuple(map(int, k.split(":"))): v for k, v in values.get('grid').items()}
        blockgrid.replace_grid(other_grid)

        response = {
            'message': "grid has been replaced",
        }
        return jsonify(response), 200

    @app.route('/grid/update', methods=['GET'])
    def update_grids():
        values = request.get_json()

        grid1 = {tuple(map(int, k.split(":"))): v for k, v in values.get('shorter_grid').items()}
        grid2 = {tuple(map(int, k.split(":"))): v for k, v in values.get('longer_grid').items()}

        response = {
            'grid': {":".join(map(str, k)): v for k, v in blockgrid.update_grid(grid1, grid2).items()},
        }
        return jsonify(response), 200

    @app.route('/nodes/register', methods=['POST'])
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
        return jsonify(response), 201

    @app.route('/nodes/resolve', methods=['GET'])
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
