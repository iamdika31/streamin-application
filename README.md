# Streaming_Application
End to End streaming Application.
<br> The goals of this application is to get how much users share spotify song/album/playlist on twitter tweets.
<br> The architecture of this streaming application is shown below
![image](https://user-images.githubusercontent.com/30191386/103266451-34128e80-49e2-11eb-8343-98206d9d20d3.png)
<br> i run this program at dataproc cluster in GCP
<br> From the raw tweets data i take 'expanded url' field and find that contains 'open.spotify.com'. 
<br> If its found then i scrapped using Jsoup and get new data from that url.
I store raw tweets data into HDFS and Elasticsearch. 
<br> For the new Information data I store it into PostgreSQL and Elasticsearch
<br> The table structure of the new information data is shown below
![image](https://user-images.githubusercontent.com/30191386/103265097-5f47ae80-49df-11eb-82d1-4c812c6409bb.png)
<br> From the result i create a simple visualization using Kibana as shown below. 
![kibana2](https://user-images.githubusercontent.com/30191386/103266053-79828c00-49e1-11eb-862d-4df7577d48e1.gif)
