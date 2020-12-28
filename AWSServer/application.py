import json
import sys
import time
import atexit
import io

from apscheduler.schedulers.background import BackgroundScheduler

from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key

from uuid import uuid4
import flask
from flask import Flask, jsonify, request, send_file
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from requests_toolbelt import MultipartEncoder
import zipfile

from sign import load_saved_keys, sign

from blockgrid import Blockgrid


def create_asset_table(dynamodb=None):
    if not dynamodb:
        with open("accesskey", "r") as ak, open("./secretkey", "r") as sk:
            dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-2.amazonaws.com",
                                      region_name='us-east-2',
                                      aws_access_key_id=ak.read(),
                                      aws_secret_access_key=sk.read())

    table = dynamodb.create_table(
        TableName='Assets',
        KeySchema=[
            {
                'AttributeName': 'name',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'time',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'time',
                'AttributeType': 'N'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    table = dynamodb.create_table(
        TableName='Grid',
        KeySchema=[
            {
                'AttributeName': 'index',
                'KeyType': 'HASH'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'index',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


dynamodb_client = boto3.client('dynamodb', region_name='us-east-2')
with open("accesskey", "r") as ak, open("./secretkey", "r") as sk:
    dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-2.amazonaws.com",
                              region_name='us-east-2',
                              aws_access_key_id=ak.read(),
                              aws_secret_access_key=sk.read())
#dynamodb.Table('Grid').delete()
#table = dynamodb.Table('Assets')
#table.delete()
try:
    create_asset_table()
except dynamodb_client.exceptions.ResourceInUseException:
    pass

table = dynamodb.Table('Assets')


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return super(DecimalEncoder, self).default(obj)


def get_app():
    application = app = Flask(__name__)
    app.json_encoder = DecimalEncoder

    limiter = Limiter(
        app,
        key_func=get_remote_address,
        #default_limits=["200 per day", "50 per hour"]
    )

    node_identifier = str(uuid4()).replace('-', '')
    blockgrid = Blockgrid()

    def remove_unused_bundles():
        filepaths = set()
        for index, _ in blockgrid.grid.items():
            for item in blockgrid.grid[index]["data"]:
                for k, v in item.items():
                    if k == "data":
                        for k2, v2, in json.loads(v).items():
                            filepaths.add(v2["filepath"])

        scan_kwargs = {
            'ProjectionExpression': "name",
        }
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            for item in response.get('Items', []):
                if item["name"] not in filepaths:
                    table.delete_item(Key={"name": item["name"]})
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=remove_unused_bundles, trigger="interval", days=3)
    scheduler.start()

    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())

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

    @app.route('/', methods=['GET'])
    def check():
        return jsonify({}), 200

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
    @limiter.limit("12 per hour")
    def new_unsigned_transaction():
        millis = int(round(time.time() * 1000))
        values = json.loads(request.form.to_dict()[None])

        for k, v in request.files.to_dict().items():
            bundle = v.read()
            chunks, chunk_size = len(bundle) // 400000, 400000
            ix = 0

            # Check if bundle has already been stored
            result = table.query(KeyConditionExpression=Key('name').eq(k + "_" + str(ix)))
            if len(result["Items"]) != 0:
                continue

            for i in range(0, chunks, chunk_size):
                table.put_item(Item={"name": k + "_" + str(ix), "time": millis,
                                     "bundle": bundle[i:i + chunk_size]})
                ix += 1

        for k, v in values.items():
            if k == "delete":
                indexes = [v2 for _, v2 in v.items()]
                for ix in indexes:
                    for d in blockgrid.grid[tuple(int(x / 100) for x in ix)]["data"]:
                        keys_to_remove = []
                        key_data = json.loads(d["data"])
                        for k2, v2 in key_data.items():
                            if "filepath" in v2 and k2 + "," + v2["filepath"] in v:
                                keys_to_remove.append(k2)
                        for key in keys_to_remove:
                            del key_data[key]
                        d["data"] = json.dumps(key_data)
                        blockgrid.table.put_item(Item={"index": str(tuple(int(x / 100) for x in ix)), "block":
                            blockgrid.grid[tuple(int(x / 100) for x in ix)]})

        indexes = {tuple(int(x / 100) for x in v["position"]): {} for _, v in values.items() if "position" in v}
        for k, v in values.items():
            if "position" in v:
                loc = tuple(int(x / 100) for x in v["position"])
                indexes[loc][k] = v

        blocks = []
        for k, v in indexes.items():
            final = {"index": k, "data": json.dumps(v), "time": millis}
            private_key, _ = load_saved_keys()
            final["signature"] = sign(private_key, final["data"].encode('utf-8')).decode('latin-1')

            # Create a new Transaction
            index = blockgrid.new_transaction(tuple(final['index']), final['data'], final['signature'], final["time"])
            blocks.append(index)

        response = {'message': f'Transaction will be added to regions {blocks}'}

        return jsonify(response), 200

    # This has to be a POST type because of unity HTTP stupidity, really should be GET
    @app.route('/grid/index', methods=['POST'])
    @limiter.limit("12 per hour")
    def data_at_index():
        values = request.get_json()

        required = ['index', 'time']
        if not all(k in values for k in required):
            return 'Missing values', 400

        index = tuple(int(x / 100) for x in values['index'])

        response = {
            'block': blockgrid.grid[index]["data"]
        }
        return jsonify(response), 200

    # This has to be a POST type because of unity HTTP stupidity, really should be GET
    @app.route('/grid/index/bundles', methods=['POST'])
    @limiter.limit("12 per hour")
    def bundles_at_index():
        values = request.get_json()

        required = ['index', 'time']
        if not all(k in values for k in required):
            return 'Missing values', 400

        index = tuple(int(x / 100) for x in values['index'])
        zb = io.BytesIO()
        bundles = set()
        with zipfile.ZipFile(zb, "a", zipfile.ZIP_DEFLATED, False) as zippedBundles:
            for item in blockgrid.grid[index]["data"]:
                for k, v in item.items():
                    if k == "data":
                        for k2, v2, in json.loads(v).items():
                            name = v2["filepath"]
                            ix = 0
                            bundle = b""
                            while True and name not in bundles:
                                out = table.query(KeyConditionExpression=Key('time').gt(values['time']) &
                                                                         Key("name").eq(name + "_" + str(ix)))['Items']
                                if len(out) == 0:
                                    break
                                bundle += out[0]["bundle"].value
                                ix += 1

                            if len(bundle) > 0:
                                bundles.add(name)
                                zippedBundles.writestr(name, io.BytesIO(bundle).getvalue(),
                                                       compress_type=zipfile.ZIP_DEFLATED)

        zb.seek(0)
        return send_file(
            io.BytesIO(zb.read()),
            attachment_filename="grid/index",
            mimetype='application/octet-stream',
            as_attachment=True
        )

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


application = get_app()
if __name__ == "__main__":
    application.run()
# app.run(host='0.0.0.0', port=5000)
