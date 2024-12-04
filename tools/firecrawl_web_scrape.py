"""
title: Firecrawl Web Scrape
description: Firecrawl web scraping tool that extracts text content using Firecrawl service.
author: Artur Zdolinski
author_url: https://github.com/azdolinski
git_url: https://github.com/azdolinski/firecrawl
required_open_webui_version: 0.4.0
requirements: requests, urllib3, pydantic, html2text
version: 0.4.0
licence: MIT
"""

import json
import logging
import asyncio
import requests
from typing import Any, Callable, List, Optional
from pydantic import BaseModel, Field
import urllib3
from bs4 import BeautifulSoup
import html2text
from pprint import pprint

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class EventEmitter:
    def __init__(self, event_emitter: Callable[[dict], Any] = None):
        self.event_emitter = event_emitter

    async def progress_update(self, description: str):
        await self.emit(description=description, status="in_progress")

    async def error_update(self, description: str):
        await self.emit(description=description, status="error", done=True)

    async def success_update(self, description: str):
        await self.emit(description=description, status="success", done=True)

    async def emit(self, description="Unknown State", status="in_progress", done=False):
        if self.event_emitter:
            await self.event_emitter(
                {
                    "type": "status",
                    "data": {"description": description, "status": status, "done": done},
                }
            )

class Tools:
    # Define Valves for admin configuration
    class Valves(BaseModel):
        # Mandatory fields
        firecrawl_api_url: str = "https://api.firecrawl.dev/v1/"
        firecrawl_api_key: str = ""
        formats: List[str] = Field(
            default=["markdown"],
            description='Output formats for the scraped content: markdown, html, rawHtml, links, screenshot'
        )

        # Optional fields with defaults
        verify_ssl: Optional[bool] = Field(
            default=True, 
            description="Whether to verify SSL certificates"
        )
        timeout: Optional[int] = Field(
            default=30, 
            description="Request timeout in seconds"
        )
        max_depth: Optional[int] = Field(
            default=2, 
            description="Maximum crawling depth for nested pages",
            alias="maxDepth"
        )
        follow_redirects: Optional[bool] = Field(
            default=True, 
            description="Whether to follow URL redirects",
            alias="followRedirects"
        )
        include_tags: Optional[List[str]] = Field(
            default=None,
            description="List of HTML tags to include in the scraping",
            alias="includeTags"
        )
        exclude_tags: Optional[List[str]] = Field(
            default=None,
            description="List of HTML tags to exclude from the scraping",
            alias="excludeTags"
        )
        headers: Optional[dict] = Field(
            default=None,
            description="Custom headers to be sent with the request"
        )
        wait_for: Optional[int] = Field(
            default=0,
            description="Time to wait before scraping in milliseconds",
            alias="waitFor"
        )

        class Config:
            populate_by_name = True
            arbitrary_types_allowed = True

        def dict(self, *args, **kwargs):
            # Get the base dictionary
            base_dict = super().dict(*args, exclude_none=True, by_alias=True, **kwargs)
            # Only include non-None and non-default values
            filtered_dict = {
                k: v for k, v in base_dict.items()
                if v is not None and not (
                    k == "timeout" and v == 30 or
                    k == "waitFor" and v == 0 or
                    k == "maxDepth" and v == 2 or
                    k == "followRedirects" and v is True or
                    k == "verify_ssl"
                )
            }
            
            # Remove empty lists or lists with empty strings
            for k in ["includeTags", "excludeTags"]:
                if k in filtered_dict and (not filtered_dict[k] or all(not x for x in filtered_dict[k])):
                    filtered_dict.pop(k)
            
            return filtered_dict

    def __init__(self):
        """Initialize the Tool with default values."""
        self.valves = self.Valves()
        self._session = None

    def html_clean_bs4(self, html_content):
        """Performs a quick cleanup of common unwanted HTML tags and attributes."""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove commonly unwanted tags (adjust as needed)
        tags_to_remove = ['script', 'style', 'head', 'iframe', 'meta', 'svg'] #Add more as you like.
        for tag in soup.find_all(tags_to_remove):
            tag.decompose()

        # Remove attributes (adjust as needed)
        attrs_to_remove = ['class', 'style'] #Add more as you like.
        for tag in soup.find_all(attrs=True):
            for attr in attrs_to_remove:
                if attr in tag.attrs:
                    del tag.attrs[attr]


        #Remove empty tags. This part is potentially fragile as it can delete empty tags you *want* to keep.
        for tag in soup.find_all():
            if not tag.contents and tag.name not in ['br', 'hr']: # Exceptions for tags that are empty by design
                tag.decompose()

        return str(soup)

    def html_clean_html2text(self, html_content):
        """Converts HTML to Markdown using html2text."""
        h = html2text.HTML2Text()

        # Podstawowa konfiguracja
        h.ignore_links = False        # zachowuje linki
        h.ignore_images = True        # ignoruje obrazy
        h.ignore_emphasis = True      # ignoruje pogrubienia/kursywy
        h.body_width = 0             # wyłącza zawijanie tekstu

        # Bardziej zaawansowana konfiguracja
        h.protect_links = True       # zachowuje pełne URLe
        h.unicode_snob = True        # zachowuje znaki unicode
        h.skip_internal_links = True # pomija wewnętrzne linki
        h.inline_links = True        # linki w tekście, nie na końcu

        h.ignore_tables = False      # zachowa tabele
        h.bypass_tables = False      # zachowa formatowanie tabel

        return h.handle(html_content)



    @property
    def session(self):
        """Get or create a requests session with proper configuration."""
        if self._session is None:
            self._session = requests.Session()
            if not self.valves.verify_ssl:
                # Disable SSL verification warnings when verify_ssl is False
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                self._session.verify = False
            if self.valves.firecrawl_api_key:
                self._session.headers.update({
                    "Authorization": f"Bearer {self.valves.firecrawl_api_key}",
                    "Content-Type": "application/json",
                })
        return self._session

    async def web_scrape(
        self, url: str, __user__: dict = None, __event_emitter__=None
    ) -> str:
        """
        Scrapes a webpage and returns its content in markdown format.
        
        :param url: The URL to scrape
        :return: The scraped content as a string
        """
        if __event_emitter__:
            event_emitter = EventEmitter(__event_emitter__)

        try:
            if __event_emitter__:
                asyncio.create_task(event_emitter.progress_update("Starting web scrape..."))

            # We need to remove all formats which are starts like: html2
            payload = {
                "url": url,
                "formats": [format for format in self.valves.formats if not format.startswith("html2")]
            }
            
            # Add optional parameters only if they're not default values
            optional_params = self.valves.dict(exclude={'firecrawl_api_url', 'firecrawl_api_key', 'formats'})
            payload.update(optional_params)

            logger.debug(f"Request payload: {payload}")

            print(f"Firecrawl Tool request for url: {url} - payload: {payload}")

            # Update status to inform user
            if __event_emitter__:
                await event_emitter.progress_update(f"Scraping content from {url}")
            
            # Make the request
            base_url = self.valves.firecrawl_api_url.rstrip('/')
            endpoint = f"{base_url}/scrape"
            logger.debug(f"Making request to endpoint: {endpoint}")
            
            response = requests.post(
                endpoint,
                json=payload,
                headers={"Authorization": f"Bearer {self.valves.firecrawl_api_key}"},
                verify=self.valves.verify_ssl,
                timeout=self.valves.timeout
            )
            logger.debug(f"Response status code: {response.status_code}")
            
            if response.status_code != 200:
                if response.status_code == 400:
                    error_msg = f"Error: Failed to scrape URL. Status code: {response.status_code} - payload send: {payload}"
                else:
                    error_msg = f"Error: Failed to scrape URL. Status code: {response.status_code}"
                if __event_emitter__:
                    await event_emitter.error_update(error_msg)
                return error_msg
            
            # Parse the response
            response_data = response.json()
            logger.debug(f"Raw response data: {response_data}")
            
            if not response_data.get("success"):
                error_msg = f"Error: {response_data.get('error', 'Unknown error occurred')}"
                if __event_emitter__:
                    await event_emitter.error_update(error_msg)
                return error_msg
            
            # Extract content based on format
            data = response_data.get("data", {}).get(self.valves.formats[0])
            
            if not data:
                error_msg = f"Error: No content found in {self.valves.formats[0]} format"
                if __event_emitter__:
                    await event_emitter.error_update(error_msg)
                return error_msg
            
            # Success message
            if __event_emitter__:
                await event_emitter.success_update(f"Firecrawl successfully scraped content from {url}")
            
            # Return the content
            content = {}  
            for format in self.valves.formats:
                data = response_data.get("data", {}).get(format)
                if data:  
                    if format == "html" and "html2text" in self.valves.formats:
                        data_html = response_data.get("data", {}).get("html")
                        content["html2text"] = str(self.html_clean_html2text(data_html))
                    
                    if format == "html" and "html2bs4" in self.valves.formats:
                        data_html = response_data.get("data", {}).get("html")
                        content["html2bs4"] = str(self.html_clean_bs4(data_html))

                    if format == "html":
                        content["html"] = str(data)
                    else:
                        content[format] = data
                    
            
            pprint(content, width=300, sort_dicts=False)
            return str(content)
            
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            logger.error(f"Exception during web scrape: {e}")
            if __event_emitter__:
                await event_emitter.error_update(error_msg)
            return error_msg

if __name__ == "__main__":
    # Disable SSL warnings for testing
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Initialize the tool
    tools = Tools()
    tools.valves.firecrawl_api_url = "https://firecrawl.selfhosted/v1/"
    tools.valves.verify_ssl = False
    tools.valves.firecrawl_api_key = "sk-1234"  # Replace with your actual API key
    tools.valves.formats = [ "html", "html2text", "html2bs4", "markdown" ]  # Support multiple formats togethere
    
    # Test payload matching the curl command
    test_url = "https://artur.zdolinski.com/"

    # Run the test
    async def run_test():
        result = await tools.web_scrape(test_url)
        print(f"\nResult:\n{result}")

    asyncio.run(run_test())
