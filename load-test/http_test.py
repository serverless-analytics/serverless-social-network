import aiohttp
import asyncio

async def main():

    async with aiohttp.ClientSession() as session:
        async with session.get('http://0.0.0.0:7072/index') as response:

            print("Status:", response.status)
            print("Content-type:", response.headers['content-type'])
            print('url is', response.url)
            html = await response.text()
            print("Body:", html[:15], "...")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
