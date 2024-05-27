import os
import asyncio
import logging
from playwright.async_api import async_playwright, TimeoutError

def configure_logger(module_name, log_file):
    logger = logging.getLogger(module_name)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

configure_logger(__name__, f"logs/{os.path.basename(__file__).split('.')[0]}.log")
logger = logging.getLogger(__name__)

base_url = "https://www.bbc.com/"
link_map = {
    "news": {
        "links": [
            # "/",
            "/topics/c2vdnvdg6xxt",
            # "/war-in-ukraine",
            # "/world/europe",
            # "/world/latin_america",
            # "/world/middle_east",
            # "/in_pictures",
            # "/reality_check"
        ],
        "select": ("a", {"data-testid": "internal-link", "class": "sc-2e6baa30-0 gILusN"})
    }
}

urls = []
for catg in link_map:
    if catg != "other":
        for suff in link_map[catg]["links"]:
            url = base_url + catg + suff
            urls.append(url)
    else:
        for suff in link_map[catg]["links"]:
            url = base_url + suff
            urls.append(url)

async def fetch_page(browser, url, retry_count=3):
    logger.info(f"Fetching url={url}")
    for attempt in range(retry_count):
        page = await browser.new_page()
        logger.info(f"New page created for url={url}, attempt={attempt + 1}")
        try:
            logger.info(f"Getting access into url={url}")
            await page.goto(url, timeout=160000)
            logger.info(f"Sleep 5s for url={url}")
            # await asyncio.sleep(5)
            init_y = await page.evaluate('window.scrollY')
            new_y = await page.evaluate('window.scrollY')
            start = True
            logger.info(f"Window scroll Y init={init_y}, new={new_y} url={url}")
            logger.info(f"Starting pagination process url={url}")
            while init_y != new_y or start:
                logger.info(f"Window scroll Y init={init_y}, new={new_y} url={url}")
                logger.info(f"Waiting for selector on {url}")
                await page.wait_for_selector('[data-testid="pagination-next-button"]', timeout=160000)
                logger.info(f"clicking on the next-button for url={url}")
                await page.click('[data-testid="pagination-next-button"]')
                # await asyncio.sleep(10) 
                logger.info(f"Fetched {url}")            
                start = False
                init_y = new_y
                new_y = await page.evaluate('window.scrollY')
            break  # exit the retry loop if successful
        except TimeoutError:
            logger.error(f"Timeout fetching {url}, attempt={attempt + 1}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        finally:
            await page.close()
    return 1

async def main():
    async with async_playwright() as p:
        logger.info("Launching the browser")
        browser = await p.chromium.launch(headless=False)
        logger.info(f"Starting batch collection, n={3}")
        for i in range(0, len(urls), 3):
            logger.info(f"Batch index={i}")
            batch = urls[i:i + 3]
            tasks = [fetch_page(browser, url) for url in batch]
            await asyncio.gather(*tasks)
        
        logger.info(f"Closing the browser")
        await browser.close()

if __name__ == "__main__":
    logger.info("Starting scrap process ...")
    asyncio.run(main())
