import pytest
from welcome_bot_app.user_storage import SqliteUserStorage, UserKey, UserProfile


# Create an in-memory SQLite database for testing
@pytest.fixture
def test_storage():
    yield SqliteUserStorage(":memory:")


def test_save_profile(test_storage):
    # Create a user profile
    user_key = UserKey(user_id=1, chat_id=1)
    profile = UserProfile(
        user_key=user_key,
        ichbin_message="Hello, world!",
        ichbin_message_timestamp=1.0,
        ichbin_message_id=123,
        local_kicked_timestamp=2.0,
        ichbin_request_timestamp=3.0,
    )

    # Save the profile
    test_storage.save_profile(profile)

    # Retrieve the saved profile
    retrieved_profile = test_storage.get_profile(user_key)

    # Check if the retrieved profile matches the original profile
    assert retrieved_profile.user_key == profile.user_key
    assert retrieved_profile.ichbin_message == profile.ichbin_message


def test_save_profile_update(test_storage):
    # Create a user profile
    user_key = UserKey(user_id=1, chat_id=1)
    profile = UserProfile(user_key=user_key, ichbin_message="Hello, world!")

    # Save the profile
    test_storage.save_profile(profile)

    # Update the profile
    profile.ichbin_message = "Goodbye, world!"
    profile.ichbin_message_timestamp = 1.0
    profile.ichbin_message_id = 123
    profile.local_kicked_timestamp = 2.0
    profile.ichbin_request_timestamp = 3.0
    test_storage.save_profile(profile)

    # Retrieve the saved profile
    retrieved_profile = test_storage.get_profile(user_key)

    # Check if the retrieved profile matches the updated profile
    assert retrieved_profile.user_key == profile.user_key
    assert retrieved_profile.ichbin_message == profile.ichbin_message


def test_get_profile(test_storage):
    # Create a user profile
    user_key = UserKey(user_id=1, chat_id=1)
    profile = UserProfile(user_key=user_key, ichbin_message="Hello, world!")

    # Save the profile
    test_storage.save_profile(profile)

    # Retrieve the saved profile
    retrieved_profile = test_storage.get_profile(user_key)

    # Check if the retrieved profile matches the original profile
    assert retrieved_profile.user_key == profile.user_key
    assert retrieved_profile.ichbin_message == profile.ichbin_message


def test_get_users_to_kick(test_storage):
    # Create user profiles
    user_key1 = UserKey(user_id=1, chat_id=1)
    profile1 = UserProfile(user_key=user_key1, ichbin_request_timestamp=1.0)

    user_key2 = UserKey(user_id=2, chat_id=2)
    profile2 = UserProfile(user_key=user_key2, ichbin_request_timestamp=2.0)

    # Ignored because ichbin_request_timestamp is greater than 2.5
    user_key3 = UserKey(user_id=3, chat_id=3)
    profile3 = UserProfile(user_key=user_key3, ichbin_request_timestamp=3.0)

    # Ignored because local_kicked_timestamp is set.
    user_key4 = UserKey(user_id=4, chat_id=4)
    profile4 = UserProfile(
        user_key=user_key4, local_kicked_timestamp=1.0, ichbin_request_timestamp=2.0
    )

    # Ignored because ichbin_message_timestamp is set.
    user_key5 = UserKey(user_id=5, chat_id=5)
    profile5 = UserProfile(
        user_key=user_key5, ichbin_request_timestamp=2.0, ichbin_message_timestamp=3.0
    )

    # Save the profiles
    test_storage.save_profile(profile1)
    test_storage.save_profile(profile2)
    test_storage.save_profile(profile3)
    test_storage.save_profile(profile4)
    test_storage.save_profile(profile5)

    # Get users to kick with a maximum ichbin_request_timestamp of 2.5
    users_to_kick = test_storage.get_users_to_kick(2.5)

    # Check if the correct users are returned
    assert len(users_to_kick) == 2
    assert user_key1 in users_to_kick
    assert user_key2 in users_to_kick
    assert user_key3 not in users_to_kick
