import os
import requests


def get_file_size_from_url(url):
    response = requests.head(url, allow_redirects=True)
    if response.status_code == 200 and "Content-Length" in response.headers:
        return int(response.headers["Content-Length"])
    return None


def does_file_exist(file_name, file_size, folder):
    # Construct the full path of the file in the folder
    target_file_path = os.path.join(folder, file_name)

    # Check if the file exists in the folder
    if os.path.isfile(target_file_path):
        # Compare sizes
        target_file_size = os.path.getsize(target_file_path)
        return file_size == target_file_size
    return False


def download_if_new_data(download_info, target_folder, **kwargs):
    download = download_info.value
    desired_filename = kwargs.get("desired_filename", download.suggested_filename)

    file_size = get_file_size_from_url(download.url)
    file_exists = does_file_exist(desired_filename, file_size, target_folder)
    if not file_exists:
        print(f"Downloading {desired_filename}")
        download.save_as(os.path.join(target_folder, desired_filename))
    else:
        print(f"🟡 Skipping download - {desired_filename} already downloaded")
