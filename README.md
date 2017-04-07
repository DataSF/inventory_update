# Inventory Update

## This respository contains various ETL jobs needed to complete the City and County of San Francisco's annual systems inventory update, as per [CA Government Code 6270.5](http://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=GOV&sectionNum=6270.5.) requiring a public inventory of "enterprise systems of record"

### Scripts/Jobs in This Repository:
* **pydev/system_updt_emails.py**:
  - Sends emails to Data Cordinators with the necessary attachments asking them to update their inventories
  - Uses the config file, jobs_config.yaml to store job parameters

* **pydev/screendoor_and_upload.py**:
  - Finds and downloads the most recent screendoor submission for each department
  - Parses the submitted excel workbooks for both the systems and inventory update data
  - Calculates various required counts and adds them to the system and inventory datasets
  - Uploads the inventory data to the open data portal
  - Sends an email notification indicating whether the ETL job successfully ran or not

* **run_job.sh**:
  - A server deployment bash script
  - Crontab needs absolute paths to run the jobs at scheduled times. The bash script calls the jobs using absolute paths
