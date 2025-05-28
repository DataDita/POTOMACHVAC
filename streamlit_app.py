import streamlit as st
import snowflake.snowpark as sp
from snowflake.snowpark.functions import col
cnx=st.connection("snowflake")
session = cnx.session()
from snowflake.snowpark import Session
from datetime import datetime, timedelta
import re
import uuid
import hashlib
import base64
from PIL import Image
import io
# Add this helper function with your imports
from PIL import Image, ImageOps
import io
# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie:cup_with_straw:")
st.write(
    f"""Choose the fruits you want in your custom smoothie!
    """
)
