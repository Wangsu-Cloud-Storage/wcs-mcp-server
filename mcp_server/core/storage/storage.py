import aioboto3
import asyncio
import logging

from typing import List, Dict, Any, Optional
from botocore.config import Config as S3Config
from urllib.parse import urlparse, quote

from ...config import config
from ...consts import consts

logger = logging.getLogger(consts.LOGGER_NAME)


class StorageService:
    """
    S3 Resource provider that handles interactions with AWS S3 buckets.
    Part of a collection of resource providers (S3, DynamoDB, etc.) for the MCP server.
    """

    def __init__(self, cfg: config.Config = None):
        """
        Initialize S3 resource provider
        """
        # Configure boto3 with retries and timeouts
        self.s3_config = S3Config(
            retries=dict(max_attempts=3, mode="adaptive"),
            connect_timeout=5,
            read_timeout=60,
            max_pool_connections=50,
        )
        self.config = cfg
        self.s3_session = aioboto3.Session()
        # 替换七牛认证为AWS凭证
        self.access_key = cfg.access_key
        self.secret_key = cfg.secret_key
        self.endpoint_url = cfg.endpoint_url
        self.region_name = cfg.region_name

    async def get_object_url(
            self, bucket: str, key: str, disable_ssl: bool = False, expires: int = 3600
    ) -> list[dict[str, Any]]:
        """
        获取对象URL
        :param disable_ssl: 是否禁用SSL
        :param bucket: 存储桶名称
        :param key: 对象键名
        :param expires: 链接有效期（秒）
        :return: dict
            返回对象信息
        """
        http_schema = "https" if not disable_ssl else "http"
        object_urls = []
        
        # 使用AWS SDK生成URL
        async with self.s3_session.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                config=self.s3_config,
        ) as s3:
            # 获取桶的访问控制配置
            try:
                acl_response = await s3.get_bucket_acl(Bucket=bucket)
                # 判断桶是否为私有
                is_private = True
                for grant in acl_response.get('Grants', []):
                    # 检查是否有公开读取权限
                    if (grant.get('Permission') == 'READ' and 
                        grant.get('Grantee', {}).get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers'):
                        is_private = False
                        break
                
                # 生成预签名URL（对私有桶）或直接URL（对公开桶）
                if is_private:
                    # 生成预签名URL
                    url = await s3.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': bucket, 'Key': key},
                        ExpiresIn=expires
                    )
                    object_urls.append({
                        "object_url": url,
                        "domain_type": "origin"
                    })
                else:
                    # 构造直接访问URL
                    url = f"{http_schema}://{self.endpoint_url.replace('https://', '').replace('http://', '')}/{bucket}/{quote(key)}"
                    object_urls.append({
                        "object_url": url,
                        "domain_type": "origin"
                    })
                    
                # 如果有配置CDN域名，也可以添加CDN URL（此处需根据实际情况配置）
                if hasattr(self.config, 'cdn_domains') and self.config.cdn_domains.get(bucket):
                    for domain in self.config.cdn_domains.get(bucket, []):
                        cdn_url = f"{http_schema}://{domain}/{quote(key)}"
                        object_urls.append({
                            "object_url": cdn_url,
                            "domain_type": "cdn"
                        })
                
            except Exception as e:
                logger.error(f"Error getting object URL: {str(e)}")
                raise
                
        return object_urls

    async def list_buckets(self, prefix: Optional[str] = None) -> List[dict]:
        """
        List S3 buckets using async client with pagination
        """
        max_buckets = 50

        async with self.s3_session.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
        ) as s3:
            if self.config.buckets:
                # If buckets are configured, only return those
                response = await s3.list_buckets()
                all_buckets = response.get("Buckets", [])

                configured_bucket_list = [
                    bucket
                    for bucket in all_buckets
                    if bucket["Name"] in self.config.buckets
                ]

                if prefix:
                    configured_bucket_list = [
                        b for b in configured_bucket_list if b["Name"] > prefix
                    ]

                return configured_bucket_list[:max_buckets]
            else:
                # Default behavior if no buckets configured
                response = await s3.list_buckets()
                buckets = response.get("Buckets", [])

                if prefix:
                    buckets = [b for b in buckets if b["Name"] > prefix]

                return buckets[:max_buckets]

    async def list_objects(
            self, bucket: str, prefix: str = "", max_keys: int = 20, start_after: str = ""
    ) -> List[dict]:
        """
        List objects in a specific bucket using async client with pagination
        Args:
            bucket: Name of the S3 bucket
            prefix: Object prefix for filtering
            max_keys: Maximum number of keys to return
            start_after: the index that list from，can be last object key
        """
        #
        if self.config.buckets and bucket not in self.config.buckets:
            logger.warning(f"Bucket {bucket} not in configured bucket list")
            return []

        if isinstance(max_keys, str):
            max_keys = int(max_keys)

        if max_keys > 100:
            max_keys = 100

        async with self.s3_session.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
        ) as s3:
            response = await s3.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys,
                StartAfter=start_after,
            )
            return response.get("Contents", [])

    async def get_object(
            self, bucket: str, key: str, max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Get object from S3 using streaming to handle large files and PDFs reliably.
        The method reads the stream in chunks and concatenates them before returning.
        """
        if self.config.buckets and bucket not in self.config.buckets:
            logger.warning(f"Bucket {bucket} not in configured bucket list")
            return {}

        attempt = 0
        last_exception = None

        while attempt < max_retries:
            try:
                async with self.s3_session.client(
                        "s3",
                        aws_access_key_id=self.access_key,
                        aws_secret_access_key=self.secret_key,
                        endpoint_url=self.endpoint_url,
                        region_name=self.region_name,
                        config=self.s3_config,
                ) as s3:
                    # Get the object and its stream
                    response = await s3.get_object(Bucket=bucket, Key=key)
                    stream = response["Body"]

                    # Read the entire stream in chunks
                    chunks = []
                    async for chunk in stream:
                        chunks.append(chunk)

                    # Replace the stream with the complete data
                    response["Body"] = b"".join(chunks)
                    return response

            except Exception as e:
                last_exception = e
                if "NoSuchKey" in str(e):
                    raise

                attempt += 1
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(
                        f"Attempt {attempt} failed, retrying in {wait_time} seconds: {str(e)}"
                    )
                    await asyncio.sleep(wait_time)
                continue

        raise last_exception or Exception("Failed to get object after all retries")

    def is_text_file(self, key: str) -> bool:
        """Determine if a file is text-based by its extension"""
        text_extensions = {
            ".txt",
            ".log",
            ".json",
            ".xml",
            ".yml",
            ".yaml",
            ".md",
            ".csv",
            ".ini",
            ".conf",
            ".py",
            ".js",
            ".html",
            ".css",
            ".sh",
            ".bash",
            ".cfg",
            ".properties",
        }
        return any(key.lower().endswith(ext) for ext in text_extensions)

    def is_image_file(self, key: str) -> bool:
        """Determine if a file is text-based by its extension"""
        text_extensions = {
            ".png",
            ".jpeg",
            ".jpg",
            ".gif",
            ".bmp",
            ".tiff",
            ".svg",
            ".webp",
        }
        return any(key.lower().endswith(ext) for ext in text_extensions)

    def is_markdown_file(self, key: str) -> bool:
        """Determine if a file is text-based by its extension"""
        text_extensions = {
            ".md",
        }
        return any(key.lower().endswith(ext) for ext in text_extensions)
