#!/usr/bin/python3


# pylint: disable=line-too-long,bad-continuation


"""
Parse HFR "COVID-19" topic and return number of post per day
"""


import re
import types
import logging
import asyncio
import datetime
import dataclasses
from typing import List, AsyncGenerator

import pytz
import pyquery
import aiohttp
import aiohttp_retry
import more_itertools


PARIS_TZ = pytz.timezone("Europe/Paris")


@dataclasses.dataclass  # pylint: disable=too-few-public-methods
class HfrPost:
    """
    Represent one single post parsed from forum HTML pages

    :param author: Post author pseudo
    :type author: str
    :param timestamp: Post timestamp, as Paris localized datetime
    :type timestamp: datetime.datetime
    """

    author: str
    timestamp: datetime.datetime


class HfrCovid19:
    """
    Parse HFR "COVID-19" topic and return number of post per day

    :param cat: Unique identifier of forum category
    :type cat: int, defaults to 13
    :param sub_cat: Unique identifier of forum sub category
    :type sub_cat: int, defaults to 422
    :param post: Unique identifier of forum topic
    :type post: int, defaults to 118532
    """

    def __init__(self, cat: int = 13, sub_cat: int = 422, post: int = 118532) -> None:
        assert isinstance(cat, int) and cat > 0, "cat parameter must be a positive integer"
        assert isinstance(sub_cat, int) and sub_cat > 0, "sub_cat parameter must be a positive integer"
        assert isinstance(post, int) and post > 0, "post parameter must be a positive integer"

        self.cat = cat
        self.sub_cat = sub_cat
        self.post = post

        self.logger = logging.getLogger(self.__class__.__name__)

        headers = {"User-Agent": "HfrCovid19Analyzer/1.0.0"}
        timeout = aiohttp.ClientTimeout(connect=3, total=30)
        self._retry_options = aiohttp_retry.ExponentialRetry(
            attempts=3, start_timeout=1, max_timeout=5.0, factor=2.0, statuses=[x for x in range(400, 600)], exceptions=[Exception]
        )
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(self._on_request_start)  # type: ignore
        self.retry_client = aiohttp_retry.RetryClient(
            timeout=timeout, headers=headers, raise_for_status=True, retry_options=self._retry_options, trace_configs=[trace_config]
        )

        self.logger.info("Initialized on cat=%d, subcat=%d, post=%d", self.cat, self.sub_cat, self.post)

    async def _on_request_start(
        self,
        session: aiohttp.ClientSession,  # pylint: disable=unused-argument
        trace_config_ctx: types.SimpleNamespace,
        params: aiohttp.TraceRequestStartParams,
    ) -> None:

        current_attempt = trace_config_ctx.trace_request_ctx["current_attempt"]
        if current_attempt > 1:
            if self._retry_options.attempts <= current_attempt:
                self.logger.info("Retrying %s at %s (%d/%d)", params.method, params.url, current_attempt, self._retry_options.attempts)
            else:
                self.logger.warning("Retrying %s at %s (%d/%d)", params.method, params.url, current_attempt, self._retry_options.attempts)

    async def close(self) -> None:
        """
        Shutdown aiohttp sessions
        """

        await self.retry_client.close()
        self.logger.info("Shut down successfully")

    async def get_page_content(self, number: int) -> str:
        """
        Get topic page number NN and return its HTML content as string

        :param number: Page number as integer
        :type number: int
        :return: Page HTML content as string
        :rtype: str
        """

        assert isinstance(number, int) and number > 0, "number parameter must be a positive integer"

        url = "https://forum.hardware.fr/forum2.php"
        params = {"config": "hfr.inc", "cat": self.cat, "subcat": self.sub_cat, "post": self.post, "page": number}
        resp = await self.retry_client.get(url, params=params)
        text = await resp.text()
        self.logger.info("Returned HTML page %d content", number)
        return text

    async def get_total_page_count(self) -> int:
        """
        Parse first page to get total number of pages

        :return: Total number of pages
        :rtype: int
        """

        page_1 = await self.get_page_content(1)
        parsed = pyquery.PyQuery(page_1)
        page_headers = parsed(".fondForum2PagesHaut .cHeader")
        page_headers_values = [x.html() for x in page_headers.items()]
        page_headers_page_count = [int(x) for x in page_headers_values if x.isdigit()]

        assert page_headers_page_count, "Unable to parse .fondForum2PagesHaut .cHeader to extract max page number"
        max_page = max(page_headers_page_count)
        self.logger.info("Topic has %d pages", max_page)
        return max_page

    @staticmethod
    def parse_page(content: str) -> List[HfrPost]:
        """
        Parse HTML page content to extract list of posts

        :param content: HTML page content as string
        :type content: str
        :return: List of dataclass representing each post present in this page
        :rtype: HfrPost
        """

        assert isinstance(content, str) and content, "content parameter must be a non-empty string"

        parsed = pyquery.PyQuery(content)
        posts = parsed(".messagetable")

        res: List[HfrPost] = []

        for post in posts.items():
            author = post(".s2").html()
            if author == "Publicité":
                continue
            timestamp_raw = post(".toolbar .left").html()
            re_matcher = re.search(r"Posté le (?P<date>[0-9]{2}-[0-9]{2}-[0-9]{4})\xa0à\xa0(?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2})", timestamp_raw)
            assert re_matcher, "Unable to parse post timestap"
            date = re_matcher.group("date")
            time = re_matcher.group("time")

            timestamp_naive = datetime.datetime.strptime(f"{date} {time}", "%d-%m-%Y %H:%M:%S")
            timestamp = PARIS_TZ.localize(timestamp_naive)

            res.append(HfrPost(author=author, timestamp=timestamp))

        return res

    async def parse_all_pages(self) -> AsyncGenerator[HfrPost, None]:
        """
        Parse all topic pages and yield HfrPost dataclasses instances representing each post

        :yield: HfrPost dataclasses instances representing each post
        """

        max_page = await self.get_total_page_count()
        coros = [(page, self.get_page_content(page)) for page in range(1, max_page + 1)]

        for pages_coros_chunk in more_itertools.chunked(coros, 100):
            pages_number_chunk = [x[0] for x in pages_coros_chunk]
            coros_chunk = [x[1] for x in pages_coros_chunk]
            pages_content = await asyncio.gather(*coros_chunk)
            for idx, page_content in enumerate(pages_content):
                posts = self.parse_page(page_content)
                self.logger.info("Got %d post for page %d", len(posts), pages_number_chunk[idx])
                for post in posts:
                    yield post


if __name__ == "__main__":

    import xlsxwriter

    async def main() -> None:
        """
        Run calculation
        """

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s")

        hfr = HfrCovid19(cat=13, sub_cat=422, post=118532)

        # Excel output
        workbook = xlsxwriter.Workbook("./posts_parsed.xlsx", {"constant_memory": True})
        worksheet = workbook.add_worksheet()
        logging.info("Writing results to ./posts_parsed.xlsx")

        try:
            row_idx = 0
            async for post in hfr.parse_all_pages():
                worksheet.write(row_idx, 0, post.author)
                worksheet.write(row_idx, 1, post.timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                row_idx += 1
            logging.info("%d posts written to ./posts_parsed.xlsx", row_idx + 1)
        finally:
            workbook.close()
            await hfr.close()

    asyncio.run(main())
