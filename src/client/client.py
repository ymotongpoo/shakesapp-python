# Copyright 2021 Yoshi Yamaguchi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import random

import flask
import grpc

import shakesapp_pb2
import shakesapp_pb2_grpc

app = flask.Flask(__name__)
logging.basicConfig(level=logging.INFO)


class ClientConfigError(Exception):
    pass


@app.route("/")
def handler():
    server_addr = os.environ.get("SERVER_ADDR", "")
    if server_addr == "":
        raise ClientConfigError("environment variable SERVER_ADDR is not set")

    channel = grpc.insecure_channel(server_addr)
    stub = shakesapp_pb2_grpc.ShakespeareServiceStub(channel)
    query = random.choice(["hello", "world", "to be, or not to be", "insolence"])
    resp = stub.GetMatchCount(shakesapp_pb2.ShakespeareRequest(query=query))
    return resp.match_count


def run():
    port = os.environ.get("PORT", "8080")
    logging.info(f"start server in Port {port}")
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    run()
