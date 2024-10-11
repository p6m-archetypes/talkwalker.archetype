from typing import List, Optional

from pydantic import BaseModel

# This class is used to represent a record from the Talkwalker API.

class Image(BaseModel):
    url: Optional[str] = None


class WorldData(BaseModel):
    continent: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    country_code: Optional[str] = None
    resolution: Optional[str] = None


class ExtraAuthorAttributes(BaseModel):
    world_data: Optional[WorldData] = WorldData()
    id: Optional[str] = None
    name: Optional[str] = None
    gender: Optional[str] = "UNKNOWN"
    image_url: Optional[str] = None
    short_name: Optional[str] = None
    url: Optional[str] = None


class ExtraSourceAttributes(BaseModel):
    world_data: Optional[WorldData] = WorldData()
    id: Optional[str] = None
    name: Optional[str] = None


class ArticleExtendedAttributes(BaseModel):
    youtube_views: Optional[int] = None
    youtube_likes: Optional[int] = None
    num_comments: Optional[int] = None
    tiktok_views: Optional[int] = None
    tiktok_likes: Optional[int] = None
    tiktok_shares: Optional[int] = None
    twitter_shares: Optional[int] = None


class NewsArticleAttributes(BaseModel):
    url: Optional[str] = None
    source: Optional[str] = None
    snippet: bool = False
    published_date: Optional[str] = None
    media: Optional[str] = None
    title: Optional[str] = None
    authors: Optional[str] = None
    text: Optional[str] = None
    summary: Optional[str] = None


class ReferencedTweet(BaseModel):
    type: str
    id: str


class Attachments(BaseModel):
    media_keys: List[str]


class PublicMetrics(BaseModel):
    retweet_count: Optional[int] = None
    reply_count: Optional[int] = None
    like_count: Optional[int] = None
    quote_count: Optional[int] = None
    bookmark_count: Optional[int] = None
    impression_count: Optional[int] = None


class ContextAnnotation(BaseModel):
    domain: Optional[dict] = {}
    entity: Optional[dict] = {}


class Author(BaseModel):
    username: Optional[str] = None
    location: Optional[str] = None
    id: Optional[str] = None
    description: Optional[str] = None
    verified: Optional[bool] = False
    name: Optional[str] = None


class TwitterData(BaseModel):
    id: Optional[str] = None
    conversation_id: Optional[int] = None
    referenced_tweets: List[ReferencedTweet] = []
    lang: Optional[str] = None
    author_id: Optional[str] = None
    created_at: Optional[str] = None
    attachments: Optional[Attachments] = None
    edit_history_tweet_ids: List[str] = []
    public_metrics: Optional[PublicMetrics] = None
    text: Optional[str] = None
    context_annotations: List[ContextAnnotation] = []
    in_reply_to_user_id: Optional[str] = None
    author: Optional[Author] = None


class TalkwalkerRecord(BaseModel):
    """
    This class represents a record from Talkwalker. It contains various fields that are used to store information
    about the record. The fields are defined using Pydantic models, which provide type checking and validation for
    the data. The class also includes methods for parsing data from raw dictionaries and serializing the data to JSON.

    All the fields in here are optional, so they can be set to None if the data is not available. However, when the
    final json is dumped we get consistent record for with default values populated for fields that are not available.
    """
    url: Optional[str] = None
    matched_profile: List[str] = []
    indexed: Optional[int] = None
    search_indexed: Optional[int] = None
    published: Optional[int] = None
    title: Optional[str] = None
    content: Optional[str] = None
    title_snippet: Optional[str] = None
    content_snippet: Optional[str] = None
    root_url: Optional[str] = None
    domain_url: Optional[str] = None
    host_url: Optional[str] = None
    parent_url: Optional[str] = None
    lang: Optional[str] = None
    porn_level: Optional[int] = None
    fluency_level: Optional[int] = None
    DEPRECATED_spam_level: Optional[int] = None
    sentiment: Optional[int] = None
    source_type: List[str] = []
    post_type: List[str] = []
    noise_level: Optional[int] = None
    noise_category: Optional[str] = None
    tokens_title: List[str] = []
    tokens_content: List[str] = []
    tokens_mention: List[str] = []
    images: List[Image] = []
    tags_internal: List[str] = []
    tags_customer: List[str] = []
    article_extended_attributes: Optional[ArticleExtendedAttributes] = None
    source_extended_attributes: Optional[ExtraSourceAttributes] = None
    extra_author_attributes: Optional[ExtraAuthorAttributes] = None
    user_response_time: Optional[int] = None
    engagement: Optional[int] = None
    reach: Optional[int] = None
    entity_url: List[Image] = []
    word_count: int = 0
    external_provider: Optional[str] = None
    external_id: Optional[int] = None
    external_author_id: Optional[int] = None
    source: Optional[str] = None
    news_article_attributes: Optional[NewsArticleAttributes] = None
    external_provider_attributes: Optional[TwitterData] = None

# TODO Clean up examples later . Keep them for testing purpose now
# record = TalkwalkerRecord(
#     url="https://example.com",
#     matched_profile=["profile1", "profile2"],
#     indexed=1234567890,
#     title="Example Title",
#     content="This is an example content.",
#     tags_internal=["tag1", "tag2"],
#     article_extended_attributes=ArticleExtendedAttributes(youtube_views=1000, youtube_likes=100),
#     extra_author_attributes=ExtraAuthorAttributes(
#         world_data=WorldData(
#             continent="North America",
#             country="United States",
#             region="California",
#             city="Los Angeles",
#             longitude=-118.2437,
#             latitude=34.0522,
#             country_code="US",
#             resolution="City"
#         ),
#         id="author123",
#         name="John Doe",
#         gender="MALE",
#         image_url="https://example.com/author.jpg",
#         short_name="jdoe",
#         url="https://example.com/author"
#     )
# )
# # print(record.model_dump_json(indent=2))
#
# data = {
#     "url": "https://example.com",
#     "matched_profile": [
#         "profile1",
#         "profile2"
#     ],
#     "indexed": 1234567890,
#     "title": "Example Title",
#     "content": "This is an example content.",
#     "source_type": [],
#     "post_type": [],
#     "tokens_title": [],
#     "tokens_content": [],
#     "tokens_mention": [],
#     "images": [],
#     "tags_internal": [
#         "tag1",
#         "tag2"
#     ],
#     "tags_customer": [],
#     "article_extended_attributes": {
#         "youtube_views": 1000,
#         "youtube_likes": 100,
#     },
#     "extra_author_attributes": {
#         "world_data": {
#             "continent": "North America",
#             "country": "United States",
#             "region": "California",
#             "city": "Los Angeles",
#             "longitude": -118.2437,
#             "latitude": 34.0522,
#             "country_code": "US",
#             "resolution": "City"
#         },
#         "id": "author123",
#         "image_url": "https://example.com/author.jpg",
#         "short_name": "jdoe",
#         "url": "https://example.com/author"
#     },
#     "user_response_time": None,
#     "engagement": None,
#     "hello": "world",
#     "not_in_model": "value",
#     "external_provider_attributes": {
#         "edit_history_tweet_ids": ["1235"],
#     }
# }

# print("Parsing from raw data")
# print(TalkwalkerRecord.parse_obj(data).model_dump_json(indent=2))
