import asyncio
import aiohttp
from bs4 import BeautifulSoup
import csv
from urllib.parse import urljoin

BASE_URL = "https://peraturan.bpk.go.id/Search?p="
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# Limit concurrency (IMPORTANT)
semaphore = asyncio.Semaphore(10)


async def fetch_page(session, page_number):
    url = BASE_URL + str(page_number)

    async with semaphore:
        try:
            async with session.get(url, headers=HEADERS) as response:
                print(f"Processing page {page_number} {response.status}...")

                results = [] 
                if response.status != 200:
                    return results

                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                container = soup.select_one(
                    "body > div > div > div:nth-of-type(2) > div:nth-of-type(2) > div:nth-of-type(2)"
                )

                if not container:
                    return results

                divs = container.find_all("div", recursive=False)

                for j in range(0, len(divs) - 1, 2):
                    item = divs[j]

                    link = item.select_one(
                        "div div div div div:nth-of-type(2) div:nth-of-type(1) a"
                    )

                    if not link:
                        continue

                    href = link.get("href")
                    if not href:
                        continue

                    results.append(urljoin("https://peraturan.bpk.go.id", href))

                return results

        except Exception as e:
            print(f"Error on page {page_number}: {e}")
            return []


async def main():
    all_results = []

    async with aiohttp.ClientSession() as session:
        tasks = []

        for i in range(4001, 30001):
            tasks.append(fetch_page(session, i))

            # Batch to avoid too many tasks in memory
            if len(tasks) >= 50:
                results = await asyncio.gather(*tasks)
                for r in results:
                    all_results.extend(r)
                tasks = []

        # Run remaining tasks
        if tasks:
            results = await asyncio.gather(*tasks)
            for r in results:
                all_results.extend(r)

    # Write once (MUCH faster)
    with open("results.csv", "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for url in all_results:
            writer.writerow([url])

    print(f"\nSaved {len(all_results)} URLs")


if __name__ == "__main__":
    asyncio.run(main())