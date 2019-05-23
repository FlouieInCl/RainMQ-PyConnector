from typing import Generator

import aiohttp
import requests


class RainMQSubscriber:
    def __init__(self, topic_name: str):
        self._set_target_topic(topic_name)

    def _set_target_topic(self, topic_name: str):
        self.topic_name = topic_name
        self.mq_url = f'localhost:8080/{self.topic_name}/bring'

    def change_target_topic(self, topic_name: str):
        self._set_target_topic(topic_name)

    def consume(self) -> Generator[dict, None, None]:
        while True:
            resp = requests.get(self.mq_url)
            message: dict = resp.json()
            if message and resp.status_code == 200:
                yield message

    async def async_consume(self) -> Generator[dict, None, None]:
        while True:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.mq_url) as resp:
                    message = await resp.json()
                    if message and resp.status:
                        yield message
