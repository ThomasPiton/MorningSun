import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any

from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.news import HeadlineNewsConfig
from morningpy.schema.news import HeadlineNewsSchema


class HeadlineNewsExtractor(BaseExtractor):
    """
    Extracts headline news from Morningstar's news API.

    This extractor handles:
        - Retrieving news headlines by edition, market, and news section
        - Processing news stories with metadata (date, title, tags, links)
        - Formatting dates and structuring data into a standardized DataFrame

    Attributes
    ----------
    edition : str
        News edition identifier (e.g., "US", "UK", "AU").
    market : str
        Market identifier for news filtering.
    news : str
        News section/category to retrieve.
    url : str
        Base API URL for news retrieval.
    params : dict
        Request parameters for API calls.
    market_id : dict
        Mapping of market names to Morningstar market IDs.
    edition_id : dict
        Mapping of edition names to Morningstar edition IDs.
    endpoint_mapping : dict
        Nested mapping of edition and news type to API endpoints.
    metadata : dict or None
        Additional metadata for the request (currently unused).
    """
    
    config = HeadlineNewsConfig
    schema = HeadlineNewsSchema
    
    def __init__(
        self,
        edition: str,
        market: str,
        news: str
    ):
        """
        Initialize the HeadlineNewsExtractor.

        Parameters
        ----------
        edition : str
            News edition identifier. Valid values depend on config
            (typically "US", "UK", "AU", etc.).
        market : str
            Market identifier for news filtering. Valid values
            depend on config.
        news : str
            News section/category to retrieve (e.g., "headlines",
            "market", "funds", "stocks").

        Notes
        -----
        The edition, market, and news parameters must match valid
        values defined in HeadlineNewsConfig. Invalid values will
        result in KeyError during request building.
        """
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        super().__init__(client)

        self.edition = edition
        self.market = market
        self.news = news
        self.url = self.config.API_URL
        self.params = self.config.PARAMS.copy()
        self.market_id = self.config.MARKET_ID
        self.edition_id = self.config.EDITION_ID
        self.endpoint_mapping = self.config.ENDPOINT
        self.columns = self.config.COLUMNS
        self.metadata = None
        
    def _check_inputs(self) -> None:
        """
        Validate user inputs for edition, market, and news type.
        
        Notes
        -----
        This method is currently a placeholder for future validation logic.
        Consider adding validation to check if edition, market, and news
        exist in their respective config mappings.
        """
        pass

    def _build_request(self) -> None:
        """
        Build the API request with edition-specific endpoint and market parameters.

        This method:
            - Resolves edition and market IDs from config mappings
            - Constructs the full API URL using edition and news type
            - Sets market-specific query parameters
            - Creates a dict-based request object for execution

        Raises
        ------
        KeyError
            If edition, market, or news type is not found in config mappings.
        """
        edition_id = self.edition_id[self.edition]
        market_id = self.market_id[self.market]
        endpoint = self.endpoint_mapping[self.edition][self.news]
        url = f"{self.url}/{edition_id}/{endpoint}"

        params = {
            **self.params, 
            "marketID": market_id,
            "sectionFallBack": self.news,
        }

        self.requests = [
            {
                "url": url,
                "params": params,
            }
        ]
        
    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar news API response into a structured DataFrame.
        
        This method:
            - Extracts news stories from the response
            - Formats ISO datetime strings to readable format
            - Aggregates tags into comma-separated string
            - Constructs full article URLs
            - Returns a standardized DataFrame with news metadata
        
        Parameters
        ----------
        response : dict
            API response dictionary containing:
            - page : dict
                Page container with stories list
                - stories : list of dict
                    List of news story objects with fields:
                    - displayDate : str (ISO format)
                    - headline : dict (title, subtitle)
                    - tags : list of dict (section names)
                    - canonicalURL : str (relative URL)
                    - language : str
                
        Returns
        -------
        pd.DataFrame
            DataFrame with columns:
            - news : str
                News section/category
            - market : str
                Market identifier
            - display_date : str
                Formatted datetime string (YYYY-MM-DD HH:MM:SS)
            - title : str
                Article headline
            - subtitle : str
                Article subtitle
            - tags : str
                Comma-separated list of tags
            - link : str
                Full URL to the article
            - language : str
                Article language code
            
            Returns empty DataFrame with defined columns if response is invalid
            or contains no stories.
        
        Examples
        --------
        >>> extractor = HeadlineNewsExtractor(edition="US", market="USA", news="headlines")
        >>> df = extractor.extract()
        >>> print(df.columns)
        Index(['news', 'market', 'display_date', 'title', 'subtitle', 'tags', 'link', 'language'])
        """
        if not isinstance(response, dict) or not response:
            return pd.DataFrame(columns=self.columns)
        
        stories = response.get('page', {}).get('stories', [])
        
        if not stories:
            return pd.DataFrame(columns=self.columns)
        
        rows = []
        
        for story in stories:

            display_date = self._format_display_date(story.get('displayDate', ''))

            headline = story.get('headline', {})
            title = headline.get('title', '')
            subtitle = headline.get('subtitle', '')

            tags = ','.join([
                section.get('name', '') 
                for section in story.get('tags', [])
                if section.get('name')
            ])

            canonical_url = story.get('canonicalURL', '')
            link = f"https://global.morningstar.com{canonical_url}" if canonical_url else ''

            language = story.get('language', '')

            rows.append({
                'news': self.news,
                'market': self.market,
                'display_date': display_date,
                'title': title,
                'subtitle': subtitle,
                'tags': tags,
                'link': link,
                'language': language
            })

        df = pd.DataFrame(rows, columns=self.columns)
        return df
    
    @staticmethod
    def _format_display_date(date_str: str) -> str:
        """
        Format ISO datetime string to readable format.
        
        Parameters
        ----------
        date_str : str
            ISO format datetime string, possibly with 'Z' suffix
            (e.g., "2024-01-15T14:30:00Z").
        
        Returns
        -------
        str
            Formatted datetime string in "YYYY-MM-DD HH:MM:SS" format,
            or original string if parsing fails.
        
        Examples
        --------
        >>> HeadlineNewsExtractor._format_display_date("2024-01-15T14:30:00Z")
        '2024-01-15 14:30:00'
        >>> HeadlineNewsExtractor._format_display_date("invalid")
        'invalid'
        """
        cleaned = date_str.replace("Z", "")
        
        try:
            dt = datetime.fromisoformat(cleaned)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, AttributeError):
            return date_str