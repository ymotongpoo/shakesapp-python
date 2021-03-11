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
import time
import urllib.parse
import urllib.request

logging.basicConfig(level=logging.INFO)


def check_client_connection(healthz_url):
    with urllib.request.urlopen(healthz_url) as resp:
        ret = resp.read().decode("utf-8")
        logging.info(f"/_healthz response: {ret}")
        if str(ret) == "ok":
            logging.info(f"confirmed connection ot clientservice")
            return True
    return False


def call_client(url):
    logging.info("call_client start")
    try:
        with urllib.request.urlopen(url) as resp:
            ret = resp.read()
            logging.info(f"count: {str(ret)}")
        logging.info("call_client end")
    except urllib.error.HTTPError as e:
        logging.warn(f"HTTP request error: {e}")


def main():
    target = os.environ.get("CLIENT_ADDR", "0.0.0.0:8080")

    # connectivity check to client service
    healthz = f"http://{target}/_healthz"
    logging.info(f"check connectivity: {healthz}")
    wait_interval = 1.0
    while not check_client_connection(healthz):
        if wait_interval > 30:
            logging.error("exponential backoff exceeded the threshold")
            return
        logging.warning(f"not connected. wait for {wait_interval}sec and retry.")
        time.sleep(wait_interval)
        wait_interval *= 3

    # start request loop to client service
    logging.info("start client request loop")
    addr = f"http://{target}"
    while True:
        call_client(addr)
        time.sleep(2.0)


if __name__ == "__main__":
    main()
