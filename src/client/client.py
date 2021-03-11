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

queries = {
    "hello": 349,
    "world": 728,
    "to be, or not to be": 1,
    "insolence": 14,
}


class ClientConfigError(Exception):
    pass


class UnexpectedResultError(Exception):
    pass


SERVER_ADDR = os.environ.get("SERVER_ADDR", "")
if SERVER_ADDR == "":
    raise ClientConfigError("environment variable SERVER_ADDR is not set")
logging.info(f"server address is {SERVER_ADDR}")


@app.route("/")
def main_handler():
    q, count = random.choice(list(queries.items()))

    channel = grpc.insecure_channel(SERVER_ADDR)
    stub = shakesapp_pb2_grpc.ShakespeareServiceStub(channel)
    resp = stub.GetMatchCount(shakesapp_pb2.ShakespeareRequest(query=q))
    if count != resp.match_count:
        raise UnexpectedResultError(
            f"The expected count for '{q}' was {count}, but result was {resp.match_count } obtained"
        )

    return str(resp.match_count)


@app.route("/_healthz")
def healthz_handler():
    return "ok"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port="8080")
