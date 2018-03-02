"""Define file paths"""

import os 

PACKAGE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

ACTIVITY_DIRECTORY = os.path.join(PACKAGE_DIRECTORY, 'user_act_json/')
DATA_DIRECTORY = os.path.join(PACKAGE_DIRECTORY, 'raw_data/')