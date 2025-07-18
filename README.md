# Memeraki Firebase CSV Uploader

This project is a set of Python tools for uploading data from CSV files directly into Firestore (Firebase) for Memeraki’s internal use. It provides separate uploaders for both artists and artforms.

## Project Overview

- **Purpose:** Automate the process of ingesting artist and artform data from CSV files to Firestore.
- **Components:**  
  - `artist` uploader  
  - `artform` uploader

You can run the corresponding uploader module to start the upload process. Each uploader is designed to read a CSV, process the records, and update Firestore under the appropriate collection.

## Prerequisites

1. **Prepare Your Service Account File**
   - Obtain the Firebase service account JSON credentials from your Firebase project settings.
   - Place the credentials file in a secure location.
   - Note the file path for use when running the uploader.

2. **CSV Files**
   - Place your CSV files containing artist or artform data in an accessible directory.
   - Ensure the files follow the schema expected by the uploader.

## How to Run the Uploaders

### 1. Artist Uploader

- **Purpose:** Upload artist data from a CSV to Firestore.
- **Run Command (from the root folder):**
    ```sh
    python -m artist.main
    ```

### 2. Artform Uploader

- **Purpose:** Upload artform data from a CSV to Firestore.
- **Run Command (from the root folder):**
    ```sh
    python -m artform.main
    ```
  Usage is similar to the artist uploader.

## Important Notes

- Only the **artist** and **artform** folders are tracked in this repository.
- Each uploader sanitizes and uploads data in batches for reliability.
- Errors and progress info will be printed to the terminal.

## Example Usage
python -m artist.main
# or for artforms
python -m artform.main


