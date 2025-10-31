import asyncio
from apify_client import ApifyClient
import json
import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv, find_dotenv

# Clear any existing environment variables
keys = ['apify_key']

for key in keys:
    if key in os.environ:
        del os.environ[key]

# Load dotenv()
try:
    load_dotenv(find_dotenv())
    print("Environment variables loaded successfully")
except Exception as e:
    print(f"An error occurred while loading the environment variables: {e}")

# Accesing Environment Variables
apify_key = os.getenv("apify_token")                                           #<---------- Testing with my free key                                    

print("Environment Variables Loaded from Functions:")
print(f"APIFY_API_KEY: {apify_key}")

# Initialize the ApifyClient with your API token
client = ApifyClient(apify_key)

def save_data_to_json(data, file_name):
    if not os.path.exists(file_name):
        # If the file does not exist, create a new one and write the data as a list
        with open(file_name, 'w') as file:
            json.dump([data], file, indent=4)
    else:
        # If the file exists, load the existing data
        with open(file_name, 'r+') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []
            
            # Append the new data to the list
            existing_data.append(data)
            
            # Set the file cursor to the beginning, truncate the file, and save the updated list
            file.seek(0)
            json.dump(existing_data, file, indent=4)
            file.truncate()

    print(f"Data saved to {file_name}")


# Function to load existing data and create sets for existing post_ids and post_dates           #-----------New function--------------#
def load_existing_data(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as file:
                post_dates = set()  # Changed to a set
                existing_data = json.load(file)
                post_ids = {post.get('id') for post in existing_data}
                post_dates_raw = {post.get('created_at') for post in existing_data}
                for guayaquil_time_str in post_dates_raw:
                    guayaquil_time = datetime.strptime(guayaquil_time_str, "%Y-%m-%d %H:%M:%S")
                    guayaquil_time_adjusted = guayaquil_time - timedelta(days=1)
                    date_str = guayaquil_time_adjusted.strftime("%Y-%m-%d")
                    post_dates.add(date_str)  # Correct usage of .add() with a set
                return post_ids, post_dates
        except json.JSONDecodeError:
            return set(), set()
    else:
        return set(), set()
   

# Initializes sets with previously calculated values ​​or with an empty set                     ------------New------------------
seen_posts, dates = load_existing_data('tiktok_posts.json')

seconds_for_next_run = 28800 # 8 hour               
#seconds_for_next_run = 200 # 8 hour               

NewerThan = max(dates) if dates else "2025-10-20"  # Define the date from which to retrieve posts     <---------------Define a new date

async def fetch_tiktok_posts():
    global NewerThan  # Declare NewerThan as global so we can modify it
    print(f"NewerThan: {NewerThan}")
    
    # Define the maximum number of posts to retrieve
    results = 30                                                                 #<------------------- To retrieve all posts 
    # Prepare the Actor input

    run_input = {                                       # New variables for tiktok api
        "excludePinnedPosts": False,
        "oldestPostDate": NewerThan,
        "profiles": [
            "danielnoboaok","presidenciaec","comunicacionec",
            "aquilesalvarz.h","alcaldiagye"
        ],
        "resultsPerPage": results,
        "shouldDownloadCovers": False,
        "shouldDownloadSlideshowImages": False,
        "shouldDownloadSubtitles": False,
        "shouldDownloadVideos": False
    }

    try:
        # Run the Tiktok Actor and wait for it to finish
        run = client.actor("OtzYfK1ndEGdwWFKQ").call(run_input=run_input)

        # Fetch and print Actor results from the run's dataset (if there are any)
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            post_id = item.get('id', None)

            # Check if the post has already been seen
            if post_id not in seen_posts and post_id is not None:
               
                tiktok_post = {}

                created_at = item.get('createTimeISO', None)

                if created_at:
                    # Parse the timestamp as a datetime object in UTC
                    utc_time = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")

                    # Define the timezone for Guayaquil, Ecuador (which is UTC-5)
                    guayaquil_tz = pytz.timezone('America/Panama')

                    # Convert the UTC time to Guayaquil time
                    guayaquil_time = utc_time.replace(tzinfo=pytz.utc).astimezone(guayaquil_tz)

                    # Subtract one day from the date
                    guayaquil_time_adjusted = guayaquil_time - timedelta(days=1)

                    # Convert to the desired format yyyy-mm-dd and add to the dates set
                    date_str = guayaquil_time_adjusted.strftime("%Y-%m-%d")
                    print(f"Post date: {date_str}")
                    dates.add(date_str)

                    guayaquil_time_str = guayaquil_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    guayaquil_time_str = "Unknown time"

                post_url = item.get('webVideoUrl', None)
                post_text = item.get('text', None)
                post_hashtags = item.get('hashtags', None)
                post_commentsCount = item.get('commentCount', None)
                post_likes = item.get("diggCount",None)
                post_shares = item.get('shareCount', None)
                post_play = item.get('playCount', None)
                post_collect = item.get('collectCount', None)
                post_mentions = item.get('mentions', None)
                post_stickers = item.get('effectStickers', None)
                post_isSlideshow = item.get('isSlideshow', None)
                post_isPinned = item.get('isPinned', None)
                post_isAd = item.get('isAd', None)


                tiktok_post['url'] = post_url
                tiktok_post['id'] = post_id
                tiktok_post['caption'] = post_text
                tiktok_post['hashtags'] = post_hashtags
                tiktok_post['commentsCount'] = post_commentsCount
                tiktok_post['likesCount'] = post_likes
                tiktok_post['created_at'] = guayaquil_time_str
                tiktok_post['sharesCount'] = post_shares
                tiktok_post['playCount'] = post_play
                tiktok_post['collectCount'] = post_collect
                tiktok_post['mentionsCount'] = post_mentions
                tiktok_post['stickers'] = post_stickers
                tiktok_post['isSlideshow'] = post_isSlideshow
                tiktok_post['isPinned'] = post_isPinned
                tiktok_post['isAd'] = post_isAd

                # Add the post ID to the seen_posts set
                seen_posts.add(post_id)
                
                print(f"Post {[post_text]} extracted")
                print(seen_posts)

                save_data_to_json(tiktok_post, 'tiktok_posts.json')

            else:
                print(f"Post {post_id} already extracted sometime ago" if post_id else "Post ID not found in the dataset")

        # Find the most recent date in the dates set
        if dates:
            NewerThan = max(dates)
            print(f"NewerThan updated to: {NewerThan}")

    except Exception as e:
        print(f"An error occurred while fetching Tiktok posts: {e}")

async def main():
    while True:
        try:
            await fetch_tiktok_posts()
            print("Waiting for 10 hours before the next execution... \n\n\n")
            await asyncio.sleep(seconds_for_next_run)  # Sleep for 10 hours (360000 seconds)
        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
            await asyncio.sleep(seconds_for_next_run)  # Wait before trying again to avoid rapid failure loop

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"An error occurred while running the main function: {e}")