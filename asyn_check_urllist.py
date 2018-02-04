# modified fetch function with semaphore
import asyncio
from aiohttp import ClientSession
from aiohttp import Timeout
import aiofiles
import async_timeout
import datetime
import re

today = datetime.date.today()
today = today.strftime("%Y%m%d")
start = datetime.datetime.now()

urllists = "urllistson.txt"
timelog = "urllistson-async-time.txt"
openedurllog = "urllistson-asyn-openedURL-%s.log" % (today)


urllist2 =[]

async def fetch(url, session):
    try:

        # async with async_timeout.timeout(30):
        # with Timeout(10):
            async with session.get(url ) as response:
                # eger response donuyor fakat connection ya yok ya da keep-alive ise dosyaya yazacak
                if response.headers.get("Connection") == "keep-alive" or response.headers.get("Connection") == None:
                    async with aiofiles.open(openedurllog, 'a') as f_handle:
                        await f_handle.write("Engellenmiyor  %s\n" % (url))
                        await f_handle.flush()
                #     print("{} with {}".format(url, delay))
                return await response.read()
    except:
        try:
                # response donsede bazen timeout dusmus gibi gozukenler icin double check
                async with session.get(url, timeout= 5) as response:
                    if response.headers.get("Connection") == "keep-alive" or response.headers.get("Connection") == None:
                        async with aiofiles.open(openedurllog, 'a') as f_handle:
                            await f_handle.write("Engellenmiyor  %s\n" % (url))
                            await f_handle.flush()
                    return await response.read()
        except:
            return 0



async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        await fetch(url, session)

def fix_url(url):
    # Eger urlin basinda http:// gibi bir protokol yok ise basina protokol koy
    return url.strip() if re.match("^(.+)://", url) else ("http://" + url.strip())


def file_len(fname):
    with open(fname) as f:
        i = 0
        for i, l in enumerate(f, 1):
            pass
    return i


async def run():

    async with aiofiles.open(urllists, mode='r') as f:
        contents = await f.readlines()

    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(500)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        for url in contents:
            # pass Semaphore and session to every GET request
            newurl = fix_url(url)
            # print(newurl)
            task = asyncio.ensure_future(bound_fetch(sem, newurl, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses


if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run())
    loop.run_until_complete(future)

    stop = datetime.datetime.now()
    duration = (stop - start).total_seconds()
    minutes, seconds = divmod(duration, 60)

    try:
        linecount = file_len(openedurllog)
    except IOError:
        # no file
        linecount = 0

    with open(timelog, "a") as file:
        file.write("%s URL Control Total Time: %d minutes %d seconds, missed url: %d\n" % (today, minutes, seconds, linecount))

    # loop.close()