# Twitter Data Science Project

This repository hosts to code for the Twitter Data Science Project. Built with [Maté](https://github.com/ilex-paraguariensis/yerbamate)

## Installation
You can use Maté to install code modules from this list:

|    | type   | name   | url                                                              | short_url                           | dependencies                                                  |
|----|--------|--------|------------------------------------------------------------------|-------------------------------------|---------------------------------------------------------------|
| 0  | data   | scrape | https://github.com/oalee/twitterds/tree/main/twitter/data/scrape | oalee/twitterds/twitter/data/scrape | ['ipdb~=0.13.9', 'snscrape~=0.6.2.20230320', 'pandas~=1.5.0'] |

## Usage
```bash
# Install the module
mate install oalee/twitterds/twitter/data/scrape
python -m twitter.data.scrape.download get
```
