# Twitter Data Science Project

This repository hosts to code for the Twitter Data Science Project. Built with [Maté](https://github.com/ilex-paraguariensis/yerbamate)

## Installation
You can use Maté to install code modules from this list (or just simply clone, copy paste and install requirements.txt inside the modules):

|    | type        | name      | url                                                                    | short_url                                 | dependencies                                                                                                                                                                                                  |
|----|-------------|-----------|------------------------------------------------------------------------|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0  | experiments | tsdae     | https://github.com/oalee/twitterds/tree/main/twitter/experiments/tsdae | oalee/twitterds/twitter/experiments/tsdae | ['ipdb~=0.13.13', 'tqdm~=4.65.0', 'sentence_transformers~=2.2.2', 'https://github.com/oalee/twitterds/tree/main/twitter/trainers/logger', 'https://github.com/oalee/twitterds/tree/main/twitter/data/loader'] |
| 1  | models      | intent    | https://github.com/oalee/twitterds/tree/main/twitter/models/intent     | oalee/twitterds/twitter/models/intent     | ['']                                                                                                                                                                                                          |
| 2  | models      | tsdae     | https://github.com/oalee/twitterds/tree/main/twitter/models/tsdae      | oalee/twitterds/twitter/models/tsdae      | ['']                                                                                                                                                                                                          |
| 3  | data        | scrape    | https://github.com/oalee/twitterds/tree/main/twitter/data/scrape       | oalee/twitterds/twitter/data/scrape       | ['numpy~=1.24.2', 'snscrape~=0.6.2.20230320', 'requests~=2.28.2', 'requests_oauthlib~=1.3.1', 'tqdm~=4.65.0', 'pandas~=1.5.3', 'ipdb~=0.13.13', 'matplotlib~=3.7.1']                                          |
| 4  | data        | visualize | https://github.com/oalee/twitterds/tree/main/twitter/data/visualize    | oalee/twitterds/twitter/data/visualize    | ['numpy~=1.24.2', 'pandas~=1.5.3', 'ipdb~=0.13.13', 'matplotlib~=3.7.1']                                                                                                                                      |
| 5  | data        | loader    | https://github.com/oalee/twitterds/tree/main/twitter/data/loader       | oalee/twitterds/twitter/data/loader       | ['numpy~=1.24.2', 'snscrape~=0.6.2.20230320', 'tqdm~=4.65.0', 'pandas~=1.5.3', 'ipdb~=0.13.13', 'sentence_transformers~=2.2.2']                                                                               |
| 6  | trainers    | logger    | https://github.com/oalee/twitterds/tree/main/twitter/trainers/logger   | oalee/twitterds/twitter/trainers/logger   | ['tqdm~=4.65.0']                                                                                                                                                                                              |
| 7  | analyze     | topic     | https://github.com/oalee/twitterds/tree/main/twitter/analyze/topic     | oalee/twitterds/twitter/analyze/topic     | ['umap_learn~=0.5.3', 'pandas~=1.5.3', 'bertopic~=0.14.1', 'ipdb~=0.13.13', 'hdbscan~=0.8.29', 'sentence_transformers~=2.2.2', 'https://github.com/oalee/twitterds/tree/main/twitter/data/loader']            |
## Usage
```bash
# Install the module
mate install oalee/twitterds/twitter/data/scrape
python -m twitter.data.scrape.download get
```

