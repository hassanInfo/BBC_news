import asyncio
import aiohttp
import os
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import tqdm
import pandas as pd
import traceback



def configure_logger(module_name, log_file):
    logger = logging.getLogger(module_name)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

configure_logger(__name__, f"logs/{os.path.basename(__file__).split('.')[0]}.log")
logger = logging.getLogger(__name__)


org_url = "https://www.bbc.com/"
api_url = "https://web-cdn.api.bbci.co.uk/xd/content-collection/"
alias = {
    "/topics/c2vdnvdg6xxt": "isreal-gaza_war",
    "/topics/ce483qevngqt": "uk_general_election",
    "/reality_check": "bbc-verify"
}
link_map = {
    "news": {
        "sfx": [
            # ("/", ),
            ("/topics/c2vdnvdg6xxt", "0c92b177-4544-4046-9b06-e428e46f72de"),
            ("/war-in-ukraine", "555e4b6e-6240-4526-8a00-fed231e6ff74"),
            ("/topics/ce483qevngqt", ),
            ("/us-canada", "db5543a3-7985-4b9e-8fe0-2ac6470ea45b"),
            ("/uk", "27d91e93-c35c-4e30-87bf-1bd443496470"),
            ("/england", "63315f03-937f-4c96-a58c-405bc8836b71"),
            ("/northern_ireland", "112246e1-959d-4bb3-b033-4853e8bfb5d5"),
            ("/scotland", "031349f4-4079-4f4d-a9b1-c67712a54e2b"),
            ("/wales", "8dd76bc6-5f9a-4628-a627-474938f3b94d"),
            ("/world/africa", "f7905f4a-3031-4e07-ac0c-ad31eeb6a08e"),
            ("/world/asia", "ec977d36-fc91-419e-a860-b151836c176b"),
            ("/world/australia", "3307dc97-b7f0-47be-a1fb-c988b447cc72"),
            ("/world/europe", "e2cc1064-8367-4b1e-9fb7-aed170edc48f"),
            ("/world/latin_america", "16d132f4-d562-4256-8b68-743fe23dab8c"),
            ("/world/middle_east", "b08a1d2f-6911-4738-825a-767895b8bfc4"),
            ("/in_pictures", "1da310d9-e5c3-4882-b7a8-ffc09608054d"),
            ("/reality_check", "9559fc2e-5723-450d-9d89-022b8458cc8d")
        ],
        "select": ("a", {"data-testid":"internal-link", "class":"sc-2e6baa30-0 gILusN"})
    },

    "sport": {
        "sfx": [
            # ("/", ),
            ("/american-football", ),
            ("/athletics", ),
            ("/basketball", ),
            ("/boxing", ),
            ("/cricket", ),
            ("/cycling", ),
            ("/darts", ),
            ("/disability-sport", ),
            ("/football", ),
            ("/formula1", ),
            ("/northern-ireland/gaelic-games", ),
            ("/golf", ),
            ("/gymnastics", ),
            ("/horse-racing", ),
            ("/mixed-martial-arts", ),
            ("/motorsport", ),
            ("/netball", ),
            ("/olympics", ),
            ("/rugby-league", ),
            ("/rugby-union", ),
            ("/snooker", ),
            ("/swimming", ),
            ("/tennis", ),
            ("/winter-sports", ),
            ("/all-sports", ),
        ],
        "select": ("div", {"class":"ssrcss-18mhvre-Promo e1vyq2e80", "data-testid":"promo"})
    },

    "business": {
        "sfx": [
            ("/", "beffe963-0dbb-4be6-9271-7b4106242d1b"),
            ("/future-of-business", "61b43b44-ce44-4a44-8633-52721125b3c7"),
            ("/technology-of-business", "43a57796-e943-46d4-9378-71c65c25f899"),
            ("/c-suite",  "beffe963-0dbb-4be6-9271-7b4106242d1b")
        ],
        "select":  ("a", {"data-testid":"internal-link", "class":"sc-2e6baa30-0 gILusN"})
    },

    "innovation": {
        "sfx": [
            ("/", "092c7c94-aa9b-4933-9349-eb942b3bde77"),
            ("/technology", "092c7c94-aa9b-4933-9349-eb942b3bde77"),
            ("/science", "3ede9019-251f-4ca5-bc05-cabba07c399e"),
            ("/artificial-intelligence", "6d032332-6ce5-425b-85a6-f260355718b3")
        ],
        "select": ("a", {"data-testid":"internal-link", "class":"sc-2e6baa30-0 gILusN"})
    },

    "travel": {
        "sfx": [
            ("/", "98529df5-2749-4618-844f-96431b3084d9"),
            ("/destinations", ),
            ("/worlds-table", "9ac84b60-229d-4821-b8b5-acd773eff973"),
            ("/cultural-experiences", "93de7bf1-db6e-4ffe-bfad-d758d5d8d6d6"),
            ("/adventures", "857e427e-fbfe-45b4-a823-a16338a697a8"),
            ("/specialist", "3762c4ea-12aa-4b4b-a878-928b740c0739")
        ],
        "select": ("a", {"data-testid":"internal-link", "class":"sc-2e6baa30-0 gILusN"})
    },

    "culture": {
        "sfx": [
            ("/", "6d50eb9d-ee20-40fe-8e0f-f506d6a02b78"),
            ("/film-tv", "472d4624-6af2-4f60-8c4e-04fbfd27b71e"),
            ("/music", "15686dd0-fe09-4c76-b945-ba46a437ef1e"),
            ("/art", "725f0e5f-3088-4d0f-8e28-e8349dd71ecc"),
            ("/style", "7f384459-da99-4f21-bdf7-dcb7da408140"),
            ("/books", "007bab80-dc46-4634-94b5-e4820f6bfd21"), 
            ("/entertainment-news", "1b3752e6-3f54-49a5-b4ac-eea4aec017aa")
        ],
        "select": ("a", {"data-testid":"internal-link", "class":"sc-2e6baa30-0 gILusN"})
    },

    "future-planet": {
        "sfx": [
            ("/", "9f0b9075-b620-4859-abdc-ed042dd9ee66"),
            ("/natural-wonders", ),
            ("/weather-science", "696fca43-ec53-418d-a42c-067cb0449ba9"),
            ("/solutions", "5fa7bbe8-5ea3-4bc6-ac7e-546d0dc4a16b"),
            ("/sustainable-business", "9f0b9075-b620-4859-abdc-ed042dd9ee66"),
            ("/green-living", "9f0b9075-b620-4859-abdc-ed042dd9ee66")
        ],
        "select": ("a", {"data-testid":"internal-link", "class":"sc-2e6baa30-0 gILusN"})
    },

    "other": {
        "sfx": [
            ("news/business/market-data", )
        ],
        "select": ("a", {"data-testid":"internal-link", "class":"sc-2e6baa30-0 gILusN"})
    }

}



async def parse_page(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            logger.info(f"Parsing page url {url}, is done!")
            content = await response.text()
            soup = BeautifulSoup(content, 'html.parser')
            return soup
        else:
            logger.error(f"Failed to retrieve the page {url}. Status code: {response.status}")

            return None



async def extract(data, session):
    logger.debug(f"data to be extracted : {data}")
    obj = data["data"]
    logger.info(f"Extracting from path {obj['path']}")
    result = {}
    result["menu"] = data["menu"]
    result["submenu"] = data["submenu"]
    result["title"] = obj["title"] if obj["title"] else ""
    result["subtitle"] = obj["summary"] if obj["summary"] else ""
    result["date"] = obj["lastPublishedAt"] if obj["lastPublishedAt"] else ""
    
    soup = await parse_page(session, urljoin(org_url, obj["path"]))
    if soup:
        img_blocks = [urljoin(org_url, img.get("src")) for img in soup.find_all("img", src=True)]
        result["images"] = img_blocks

        aut_blocks = [block.text for block in soup.find_all("div", {"data-component": "author-block"})]
        result["authors"] = aut_blocks

        text_blocks = [block.text for block in soup.find_all("div", {"data-component": "text-block"})]
        result["text"] = text_blocks

    return result




def get_ref_url(link_map, alias):
    only_refs = []
    with_urls = []
    for menu in link_map:
        if menu != "other":
            for sfx in link_map[menu]["sfx"]:
                ref = org_url + menu + sfx[0]           
                if len(sfx) > 1:
                    url = api_url + sfx[1]  
                    if sfx[0] in alias:   
                        with_urls.append((ref, url, menu, alias[sfx[0]]))
                    else:
                        with_urls.append((ref, url, menu, sfx[0].split('/')[-1]))
                else:
                    if sfx[0] in alias:   
                        only_refs.append((ref, menu, alias[sfx[0]]))
                    else:
                        only_refs.append((ref, menu, sfx[0].split('/')[-1]))

        # else:
        #     for sfx in link_map[menu]["sfx"]:
        #         ref = org_url + sfx[0]
        #         if len(sfx) > 1:
        #             url = api_url + sfx[1]  
        #             with_urls.append((ref, url))
        #         else:
        #             only_refs.append(ref)
    
    return with_urls, only_refs 




async def get_data_from_api(session, t, retry_count=3, retry_delay=2):
    logger.info(f"Getting the data from api, url={t[0]}!!")
    all_data = []
    headers = {
        'accept': '*/*',
        'accept-language': 'fr,fr-FR;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'origin': 'https://www.bbc.com',
        'priority': 'u=1, i',
        'referer': t[0]
    }
    page = 0
    while True:
        try:
            async with session.get(t[1], params={'page': page}, headers=headers) as response:
                response.raise_for_status() 
                data = await response.json()
                logger.debug(f"Fetched data for page {page}")
                if len(data["data"]) < 1:
                    return all_data
                else:
                    for obj in data["data"]:
                        all_data.append({"data":obj,
                                        "menu":t[2],
                                        "submenu":t[3]})
                    page += 1
        except aiohttp.ClientResponseError as e:
            if e.status == 429:  
                if retry_count > 0:
                    retry_count -= 1
                    logger.warning(f"Received 'Too Many Requests' error (page {page}). Retrying after {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Exceeded retry limit for 'Too Many Requests' error on page {page}")
                    raise  
        except Exception as e:  
            logger.error(f"An error occurred fetching data: {traceback.format_exc()}")
            raise  




def save(data):
    df = pd.DataFrame(data)
    df.to_csv('./temp/scraped_data.csv', mode='a', sep='|', index=False, header=False)



async def fetch_all(list_obj, batch_size):
    logger.info("Start extracting data...")
    logger.debug(f"Total number of pages : {len(list_obj)}")
    logger.debug(f"Batch size : {batch_size}")
    logger.debug(f"Number of batches : {len(list_obj)/batch_size}")
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(list_obj), batch_size):
            logger.debug(f"Batch {i}")
            batch = list_obj[i:i + batch_size]
            tasks = [extract(obj, session) for obj in batch]
            results = await asyncio.gather(*tasks)
            save(results)




async def main():
    logger.info("Start preparing urls from defined spider map...")
    with_urls, only_refs = get_ref_url(link_map, alias)

    logger.info("Start Getting data from api...")
    api_data = []
    async with aiohttp.ClientSession() as session:
        tasks = [get_data_from_api(session, t) for t in with_urls]
        api_data = await asyncio.gather(*tasks)

    logger.info("Successfully retrieving data from the API!")
    logger.info("Flattening a list")
    list_obj = [item for l1 in api_data for item in l1]

    await fetch_all(list_obj, batch_size=10)




if __name__ == "__main__":
    asyncio.run(main())
    logger.info("Finished !!!")