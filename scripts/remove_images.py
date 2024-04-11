import os
import shutil
import logging
import concurrent
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import argparse
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def delete_file(file_path):
    """Delete a single file and log the action."""
    try:
        os.remove(file_path)
        #logging.info(f"Deleted: {file_path}")
        return True
    except Exception as e:
        logging.error(f"Error deleting {file_path}: {e}")
        return False



def get_files(directory):

    def is_file_and_path(file):
        filepath = os.path.join(directory, file)
        if os.path.isfile(filepath):
            return filepath
        return None

    files = []
    # Using ThreadPoolExecutor to handle file system operations concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Create future tasks for each file in the directory listing
        futures = [executor.submit(is_file_and_path, file) for file in os.listdir(directory)]
        # As each future completes, check if it returned a filepath and add it to the list
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                files.append(result)
    return files

def delete_files_in_directory(directory, update_interval=1000):
    """Delete all files in the specified directory using parallel execution and display a progress bar."""
    if not os.path.isdir(directory):
        logging.error(f"The specified directory does not exist: {directory}")
        return

    # List all file paths
    files = get_files(directory)
    #files = [os.path.join(directory, file) for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]



    total_files = len(files)

    print(f'Deleting {total_files} files...')

    start = time.time()

    # Initialize a progress bar
    progress_bar = tqdm(total=total_files, desc="Deleting Files", unit="file")

    # Use ThreadPoolExecutor to delete files in parallel
    with ThreadPoolExecutor() as executor:
        # Future-to-file mapping
        future_to_file = {executor.submit(delete_file, file): file for file in files}
        completed = 0
        for future in concurrent.futures.as_completed(future_to_file):
            completed += 1
            if completed % update_interval == 0 or completed == total_files:
                progress_bar.update(update_interval if completed % update_interval == 0 else completed % update_interval)
    
    progress_bar.close()

    end = time.time()


    print(f'Finished, it took {round((end-start)/60,1)} minutes')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input", type=str, required=True)
    #parser.add_argument("--processes", type=int, nargs = '?', const=6)
    args = parser.parse_args()

    directory_path = args.input
    delete_files_in_directory(directory_path)
