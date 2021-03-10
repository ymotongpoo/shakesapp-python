#!/usr/bin/env python
#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from locust import HttpUser, TaskSet, between


def request(loc):
    loc.client.get("/")


class LoadGenerator(TaskSet):
    def on_start(self):
        request(self)

    tasks = {request: 4}


class WebsiteUser(HttpUser):
    tasks = [LoadGenerator]
    wait_time = between(2, 10)