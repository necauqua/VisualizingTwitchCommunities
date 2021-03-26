import asyncio
import logging
import math

logger = logging.getLogger('twitch')

TWITCH_PAGE_SIZE = 100


# Gets the count top streams currently live on twitch
async def get_top_streamers(session, credentials, count=20):
    """
    Gets the `count` top streams currently live on twitch.

    This endpoint is paginated, so it would make `ceil(count / 100)`
    sequential requests, which can get slow pretty fast.

    Due to obvious races the return can contain duplicates at page borders,
    this method does not check for that.

    Also can sometimes return slightly less than the requested count due
    to Twitch being weird and just returning slightly less sometimes.
    """

    logger.info('Getting top %d live streams from Twitch', count)

    headers = {
        'Client-ID': credentials['client-id'],
        'Authorization': f"Bearer {credentials['access-token']}",
    }

    result = []
    cursor = ''

    while count != 0:
        batch = min(count, TWITCH_PAGE_SIZE)
        url = f'https://api.twitch.tv/helix/streams?first={batch}&after={cursor}'
        count -= batch

        logger.debug(f'Requesting a batch of {batch} top streamers, {math.ceil(count / TWITCH_PAGE_SIZE)} batches left')

        async with session.get(url, headers=headers) as response:
            data = await response.json()

            # only error responses have a message field in them
            if msg := data.get('message'):
                raise Exception(msg)

            cursor = data['pagination']['cursor']

            result.extend(element['user_login'] for element in data['data'])

    return result


async def get_current_viewers(session, channel):
    """
    Get a list of viewers for a given twitch channel from tmi.twitch.tv (not a part of the documented API).
    """

    async with session.get(f'http://tmi.twitch.tv/group/user/{channel.lower()}/chatters') as r:
        data = await r.json()
        viewers = [viewer for group in data['chatters'].values() for viewer in group]

        logger.debug('Got %d viewers for %s', len(viewers), channel)

        return viewers


# This method looks up the viewers of each streamer and creates a dictionary of {streamer: [viewers]}
async def get_viewer_map(session, streamers):
    """
    Asynchronously gets lists of current viewers for each of the streamers in the list.
    """

    logger.info('Gathering a viewer map for %d streamers', len(streamers))

    data = {}

    async def add_viewers_task(streamer):
        data[streamer] = await get_current_viewers(session, streamer)

    # run every task in parallel and wait for the results
    await asyncio.gather(*map(add_viewers_task, streamers), return_exceptions=True)

    logger.info('Finished gathering the viewer map')

    return data
