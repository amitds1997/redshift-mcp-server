import unittest
from datetime import UTC, datetime

from pydantic import BaseModel, Field

from cache_utils import cached_with_force_refresh


class UserInfo(BaseModel):
    user_id: int
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


@cached_with_force_refresh(ttl=60, force_arg_name="force_refresh")
async def dummy_function(user_id: int, force_refresh: bool = False) -> UserInfo:
    return UserInfo(user_id=user_id, created_at=datetime.now(UTC))


class TestCacheUtils(unittest.IsolatedAsyncioTestCase):
    async def test_cached_with_force_refresh(self):
        response1 = await dummy_function(1)
        self.assertEqual(response1.user_id, 1)

        response2 = await dummy_function(1)
        self.assertEqual(response2.user_id, 1)
        self.assertEqual(response1.created_at, response2.created_at)

        response3 = await dummy_function(1, force_refresh=True)
        self.assertEqual(response3.user_id, 1)
        self.assertNotEqual(response1.created_at, response3.created_at)

        response4 = await dummy_function(1)
        self.assertEqual(response4.user_id, 1)
        self.assertEqual(response3.created_at, response4.created_at)
