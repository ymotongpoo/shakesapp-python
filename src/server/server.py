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
import re
from concurrent import futures

import grpc
from google.cloud import storage

import shakesapp_pb2
import shakesapp_pb2_grpc

BUCKET_NAME = "dataflow-samples"
BUCKET_PREFIX = "shakespeare/"


class ShakesappService(shakesapp_pb2_grpc.ShakespeareServiceServicer):
    """ShakesappService accepts request from the clients and search query
    string from Shakespare works fetched from GCS.
    """

    def __init__(self):
        super().__init__()

    def GetMatchCount(self, request, context):
        texts = read_files_multi()
        count = 0
        """TODO: intentionally implemented in inefficient way.
        """
        for text in texts:
            for line in text.split("\n"):
                line, query = line.lower(), request.query.lower()
                matched = re.match(query, line)
                if matched:
                    count += 1
        return shakesapp_pb2.ShakespeareResponse(match_count=count)


def read_files_multi():
    """read_files_multi fetchse Shakespeare works from GCS in multi threads.

    TODO: This part should be multiprocess.
    """
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    itr = client.list_blobs(bucket, prefix=BUCKET_PREFIX)
    blobs = list(itr)

    executor = futures.ThreadPoolExecutor(max_workers=8)
    results = []
    for blob in blobs:
        ret = executor.submit(blob.download_as_bytes)
        results.append(ret)
    executor.shutdown()
    return [str(r.result()) for r in results]


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    shakesapp_pb2_grpc.add_ShakespeareServiceServicer_to_server(
        ShakesappService, server
    )

    port = os.getenv("PORT")
    if port == "":
        port = 5050
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
