#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import cv2
from cv2 import VideoWriter_fourcc, VideoWriter
import sys
import os
import requests
import v3io_frames as v3f
import base64
import time


# In[ ]:


import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "http://%s/%s/%s/"% (os.getenv('V3IO_WEBAPI'),os.getenv('IGZ_CONTAINER'),os.getenv('RAW_VIDEO_STREAM'))
headers = {
            "Content-Type": "application/json",
            "X-v3io-function": "PutRecords",
            "X-v3io-session-key" : os.getenv("V3IO_ACCESS_KEY")
          }


# In[2]:


def stream_frame_write(cameraID,payload):
    bef = time.time()
    r = requests.post(url, headers=headers,json=payload, verify=False)   
    time_diff = time.time()-bef
    print("Post time %s. Response %s"% (time_diff, r.text))
    return r.text


# In[1]:


def start_capture(cameraID: str,
                cameraURL:str,
                shard: int):

    # To capture video from webcam.
    cap = cv2.VideoCapture(cameraURL)
    # To use a video file as input
    # cap = cv2.VideoCapture('filename.mp4')
    data_count = 1
    while True:

        # Display
        #cv2.imshow('img', img)

        fourcc = VideoWriter_fourcc(*'MPEG')
        running_size=0
        Records=[]
        while (cap.isOpened()):
            ret, img = cap.read()
            ret, buffer = cv2.imencode('.jpg', img)
            data = base64.b64encode(buffer)
            Records.append({
                    "Data":  data.decode('utf-8'),
                    "ShardId" : shard
                    })
            if data_count == 60:
                try:
                    payload = {"Records": Records}
                    r = stream_frame_write(cameraID,payload)
                except:
                    print("Failed to write to shard %s"% shard)
                data_count = 1


        # Stop if escape key is pressed
        #k = cv2.waitKey(0) & 0xff
        #if k==27:
        #    break
    # Release the VideoCapture object
    cap.release()
    


# In[4]:


def get_cameras_list():
    client = v3f.Client(os.getenv('V3IO_FRAMES'),container=os.getenv('IGZ_CONTAINER'))
    df=client.read('kv',os.getenv('CAMERA_LIST_TBL'))
    return df


# In[5]:


def init_function():
    cameraID = os.getenv('cameraID')
    shardId = int(os.getenv('shardId'))
    cameraURL = os.getenv('cameraURL')
    
    if isinstance(cameraURL, int):
        cameraURL = int(cameraURL)
        
    cameras_list = get_cameras_list()
    for index, row in get_cameras_list().iterrows():
        if index == cameraID  and row['shard'] == shardId and row['url'] == cameraURL and row['active'] == True:
            start_capture(cameraID,cameraURL,shardId)
    print("Invalid camera")


# In[ ]:


init_function()


# Variables needed for container operations
# 
# V3IO_ACCESS_KEY
# 
# V3IO_USERNAME
# 
# V3IO_WEBAPI
# 
# V3IO_FRAMES 
# 
# IGZ_CONTAINER
# 
# RAW_VIDEO_STREAM
# 
# CAMERA_LIST_TBL
# 
# shardId
# 
# cameraID
# 
# cameraURL

# In[ ]:




