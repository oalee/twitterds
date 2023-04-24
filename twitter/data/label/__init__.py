

import os
import openai, ipdb
openai.organization = "org-gHXQknHtUk2JCL8YfNmC1RVC"
openai.api_key = os.getenv("OPEN_API_KEY")


openai.Model.list()



ipdb.set_trace()
