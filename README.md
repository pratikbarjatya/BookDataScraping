# BookDataScraping

## Part I. Data Collection: 
- Created a scraping infra to scrape the following website:
    - http://books.toscrape.com/
- The data contains the following 2 classes with the features listed:
    - Books
        - Name
        - Image Link
        - Rating
        - Price
        - In-Stock or not
        - Category
    - Category
        - Name
        - Link

## Part II. Data Warehousing:
- Created a sqlite database `pythonsqlite.db` a warehousing infra to store the above scraped data in a
sqlite. 
- This database contains two tables:
    - books_table 
    - category_table 


## Tech Stack
- Programming Language: Python 3
- Database: Sqlite3
- Version Control: git

## Enhancement 
- Store the data in Cloud Object Stores like S3. 
A method to write data is defined, providing the configuration details in config
 this can be used to save the file in cloud store s3  
- Airflow like scheduler can be used to configure and trigger this job.