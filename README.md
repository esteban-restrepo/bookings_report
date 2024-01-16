#Winks Web Scraper For the Occupancy Report

## Introduction

Welcome to our Python-based web scraper! This tool is designed to fetch the occupancy report from Winks. Before you dive in, it's essential to configure the scraper according to your needs. This README will guide you through the process.

## Getting Started


### Prerequisites

- Python 3.x installed on your machine
- Pip package manager
- beautifulsoup 4.12.x
- Pandas 2.x
- numpy 1.2.x
- rich 13.x
- python-dateutil 2.8.x

### Installation

Just extract the files in a local directory and make sure you have well configured the config.yaml file and execute the script

- python occupancy_report_downloader.py

In case you have the standalone version (occupancy_report_downloader.exe) just execute it 

### Configuration

The scraper relies on a configuration file (config.yaml) to tailor its operation to your specific requirements. Follow these steps to set up your configuration:

Open the config.yaml file in a text editor of your choice.

- Introduce your user and password
- Customize the configuration for each hotel (code, name, currency)
- Define the data range

### Additional Notes

Ensure that your credentials are kept confidential. Do not share the config.yaml file or your login details.

Be mindful of the frequency of your requests to avoid any disruptions.

Happy scraping! If you encounter any issues or have questions, feel free to reach out to Esteban Restrepo esteban.restrepo@selina.com