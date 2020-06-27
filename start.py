from uuid import uuid4
from flask import Flask, jsonify, request

import math

from blockgrid import Blockgrid

app = Flask(__name__)
node_identifier = str(uuid4()).replace('-', '')
blockgrid = Blockgrid()


@app.route('/mine', methods=['GET'])
def mine():
    # We run the proof of work algorithm to get the next proof...
    values = request.get_json()

    index = tuple(values["index"])

    if index not in blockgrid.grid:
        return 'Previous block has not been mined', 400

    block = blockgrid.grid[index].copy()
    block["owner"] = values["signature"]
    last_proof = blockgrid.hash_without_proof(block)
    proof = blockgrid.proof_of_work(last_proof, index)

    # Forge the new Block by adding it to the chain
    blockgrid.grid[index]["owner"] = values["signature"]
    blockgrid.grid[index]["proof"] = proof
    previous_hash = blockgrid.hash(blockgrid.grid[index])

    for i in range(len(index)):
        for j in (-1, 1):
            l_index = list(index)
            l_index[i] += j
            new_index = tuple(l_index)
            if new_index not in blockgrid.grid:
                blockgrid.new_block(new_index, previous_hash)

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
    return jsonify(response), 200


@app.route('/grid', methods=['GET'])
def full_grid():
    response = {
        'chain': {":".join(map(str, k)): v for k, v in blockgrid.grid.items()},
        'length': len(blockgrid.grid),
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
