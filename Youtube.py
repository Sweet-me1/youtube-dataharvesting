# Import the googleapiclient library
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

#Api key connection 
def Api_connect():
    Api_Id="AIzaSyBJsZn4ots6tjhTWAogQhhyHfI9d-UfJxg"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()

#Channel information
def get_channel_info(channel_Id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_Id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i["statistics"]["subscriberCount"],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_ID=i["contentDetails"]["relatedPlaylists"]["uploads"],
                )
        return data
    
#get video ids
def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token= None

    while True:
        response1 = youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
                break
    return video_ids


#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'],
                    )  
            video_data.append(data) 
    return video_data
        

#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                Comment_data.append(data)
    except:
        pass
    return Comment_data


#get_playlist_details

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_count=item['contentDetails']['itemCount'],
                                )
                        All_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data        


#upload to mongoDB
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                       "video_information":vi_details,"comment_information":com_details})
    return "upload completed successfully"


# Table creation for channels, playlist, videos, comments
def channels_table():
    # MySQL connection parameters
    conn_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'youtube_data',
        'port': 3306,
    }

    # Establish a connection
    conn = mysql.connector.connect(**conn_params)

    # Create a cursor
    cursor = conn.cursor()

    drop_query = '''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)

    # Commit changes after DROP TABLE
    conn.commit()
    print("create table")
    create_query = '''CREATE TABLE IF NOT EXISTS channels(
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(80) PRIMARY KEY,
                        Subscribers BIGINT,
                        Views BIGINT,
                        Total_Videos INT,
                        Channel_Description TEXT,
                        Playlist_ID VARCHAR(80)
                    )'''
    cursor.execute(create_query)

    # Commit changes after CREATE TABLE
    conn.commit()

    ch_list = []
    # Assuming 'client' is a valid MongoDB client
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    
    try:
        for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
            ch_list.append(ch_data["channel_information"])

        df = pd.DataFrame(ch_list)

        for index, row in df.iterrows():
            insert_query = '''INSERT INTO channels(
                                Channel_Name,
                                Channel_Id,
                                Subscribers,
                                Views,
                                Total_Videos,
                                Channel_Description,
                                Playlist_ID
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

            values = (
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_ID']
            )

            cursor.execute(insert_query, values)
            conn.commit()

        print("Channels values inserted successfully")

    except Exception as e:
        print(f"Error inserting channels values: {e}")


def playlist_table():
    
        # MySQL connection parameters
        conn_params = {
            'host': 'localhost',
            'user': 'root',
            'password': '12345678',
            'database': 'youtube_data',
            'port': 3306,
        }

        # Establish a connection
        conn = mysql.connector.connect(**conn_params)

        cursor = conn.cursor()

        # Drop the table if it exists
        drop_query = '''DROP TABLE IF EXISTS playlists'''
        cursor.execute(drop_query)
        conn.commit()

        # Create the 'playlists' table
        create_query = '''CREATE TABLE IF NOT EXISTS playlists(
                            Playlist_Id VARCHAR(100) PRIMARY KEY,
                            Title VARCHAR(100),
                            Channel_Id VARCHAR(100),
                            Channel_Name VARCHAR(100),
                            PublishedAt TIMESTAMP,
                            Video_count INT
                        )'''
        cursor.execute(create_query)
        conn.commit()

        # Fetch data from MongoDB
        pl_list = []
        db = client["Youtube_data"]
        coll1 = db['channel_details']
        for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
            for i in range(len(pl_data.get("playlist_information", []))):
                pl_list.append(pl_data["playlist_information"][i])

        df1 = pd.DataFrame(pl_list)

        print(df1)

        # Convert 'PublishedAt' to datetime
        df1['PublishedAt'] = pd.to_datetime(df1['PublishedAt'])

        print("DataFrame after converting PublishedAt:")
        print(df1)

        # Insert data into the 'playlists' table
        for index, row in df1.iterrows():
            insert_query = '''INSERT INTO playlists(
                                Playlist_Id,
                                Title,
                                Channel_Id,
                                Channel_Name,
                                PublishedAt,
                                Video_count
                            ) VALUES (%s, %s, %s, %s, %s, %s)'''
            values = (
                row.get('Playlist_Id', ''),
                row.get('Title', ''),
                row.get('Channel_Id', ''),
                row.get('Channel_Name', ''),
                row.get('PublishedAt', ''),
                row.get('Video_count', '')
            )

            cursor.execute(insert_query, values)
            conn.commit()
            

#call the video function table
def videos_table():
# MySQL connection parameters
    conn_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'youtube_data',
        'port': 3306,
    }
    
    try:

        # Establish a connection
        conn = mysql.connector.connect(**conn_params)    
        cursor=conn.cursor()

        drop_query='''drop table if exists videos'''
        cursor.execute(drop_query)
        conn.commit()

        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                Channel_Id varchar(100),
                                                Video_Id varchar(30) primary key,
                                                Title varchar(150),
                                                Tags text,
                                                Thumbnail varchar(200),
                                                Description text,
                                                Published_Date timestamp,
                                                Duration varchar(10),
                                                Views bigint,
                                                Likes bigint,
                                                Comments int,
                                                Favorite_Count int,
                                                Definition varchar(10),
                                                Caption_Status varchar(50)
                                                )'''
        cursor.execute(create_query)
        conn.commit()

        vi_list=[]
        db=client["Youtube_data"]
        coll1=db['channel_details']
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range (len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
        df2=pd.DataFrame(vi_list)

        df2['Published_Date'] = pd.to_datetime(df2['Published_Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
        for index, row in df2.iterrows():
        # ... (existing code)
            # Replace 'None' with 0 for integer columns
            values = tuple(0 if pd.isna(value) else value for value in row[['Views', 'Likes', 'Comments', 'Favorite_Count']].values)


            insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                            on duplicate key update
                                                Title=values(Title),
                                                Tags=values(Tags),
                                                Thumbnail=values(Thumbnail),
                                                Description=values(Description),
                                                Published_Date=values(Published_Date),
                                                Duration=values(Duration),
                                                Views=values(Views),
                                                Likes=values(Likes),
                                                Comments=values(Comments),
                                                Favorite_Count=values(Favorite_Count),
                                                Definition=values(Definition),
                                                Caption_Status=values(Caption_Status)'''
                    
            values = tuple(str(value) if value is not None else 0 for value in (row['Channel_Name'],
                                                                                row['Channel_Id'],
                                                                                row['Video_Id'],
                                                                                row['Title'],
                                                                                row['Tags'],
                                                                                row['Thumbnail'],
                                                                                row['Description'],
                                                                                row['Published_Date'],
                                                                                row['Duration'],
                                                                                row['Views'],
                                                                                row['Likes'],
                                                                                row['Comments'],
                                                                                row['Favorite_Count'],
                                                                                row['Definition'],
                                                                                row['Caption_Status']
                                                                                ))

            cursor.execute(insert_query, values)
            conn.commit()

        print("Videos table created successfully")

    except Exception as e:
        print(f"Error creating videos table: {e}")


def comments_table():
    # MySQL connection parameters
    conn_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'youtube_data',
        'port': 3306,
    }

    try:
        # Establish a connection
        conn = mysql.connector.connect(**conn_params)
        cursor = conn.cursor()

        drop_query = '''drop table if exists comments'''
        cursor.execute(drop_query)
        conn.commit()

        create_query = '''create table if not exists comments(Comment_Id varchar(100) primary key,
                            Video_Id varchar(50),
                            Comment_Text text,
                            Comment_Author varchar(150),
                            Comment_Published timestamp )'''

        cursor.execute(create_query)
        conn.commit()

        com_list = []
        db = client["Youtube_data"]  # Assuming 'client' is a valid MongoDB client
        coll1 = db['channel_details']
        for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
            for i in range(len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
        df3 = pd.DataFrame(com_list)

        df3['Comment_Published'] = pd.to_datetime(df3['Comment_Published']).dt.strftime('%Y-%m-%d %H:%M:%S')
        for index, row in df3.iterrows():
            insert_query = '''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published
                                                )
                                                values(%s,%s,%s,%s,%s)
                                                on duplicate key update
                                                        Comment_Id=values(Comment_Id),
                                                        Video_Id=values(Video_Id),
                                                        Comment_Text=values(Comment_Text),
                                                        Comment_Author=values(Comment_Author),
                                                        Comment_Published=values(Comment_Published)'''

            values = tuple(str(value) for value in (row['Comment_Id'],
                                                    row['Video_Id'],
                                                    row['Comment_Text'],
                                                    row['Comment_Author'],
                                                    row['Comment_Published']
                                                    ))
            cursor.execute(insert_query, values)
            conn.commit()

        print("Comments table created successfully")

    except Exception as e:
        print(f"Error creating comments table: {e}")


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"


def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    
    df = pd.DataFrame(ch_list)
    st.table(df)

    

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)
    st.table(df1)
  


def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range (len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)
    st.table(df2)

      
def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range (len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)
    st.table(df3)

      

#streamlit part

with st.sidebar:
    st.title(":blue[YOUTUBE HARVESTING AND WAREHOUSING]")
    st.header("Skills Take Away")
    st.caption("python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the Channel ID")

if st.button("Collect and Store Data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()
elif show_table == "PLAYLISTS":
    show_playlists_table()
elif show_table == "VIDEOS":
    show_videos_table()
elif show_table == "COMMENTS":
    show_comments_table() 


#SQL Connection
conn = mysql.connector.connect(
    host="localhost",
    database="youtube_data",
    user="root",
    password="12345678",
    port="3306")

cursor=conn.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. Channels with most number of videos",
                                              "3. Top 10 most viewed videos",
                                              "4. Comments in each videos",
                                              "5. Videos with highest likes",
                                              "6. Likes of All Videos",
                                              "7. Views of each Channel",
                                              "8. Videos Published in the year of 2008",
                                              "9. Average duration of all videos in each channel",
                                              "10. Videos with highest number of comments"))

# Execute the selected query based on the chosen question
if question.startswith("1. All the videos and the channel name"):
    query = '''select title as videos, channel_name as channelname from videos'''
    cursor.execute(query)
    results = cursor.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["video title", "channel name"])
    st.write(df)

    
elif question.startswith("2. Channels with most number of videos"):
    query2='''select channel_name as channelname,total_videos as no_videos from channels order by total_videos desc'''
    cursor.execute(query2)
    t2=cursor.fetchall()
    conn.commit()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"]) 
    st.write(df2)

    
elif question.startswith("3. Top 10 most viewed videos"):
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3=cursor.fetchall()
    conn.commit()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question.startswith("4. Comments in each videos"):
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    t4=cursor.fetchall()
    conn.commit()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

    
elif question.startswith("5. Videos with highest likes"):
    query5='''select title as videotitle,channel_name as channelname, likes as likescount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    t5=cursor.fetchall()
    conn.commit()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

    
elif question.startswith("6. Likes of All Videos"):
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    t6=cursor.fetchall()
    conn.commit()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

  
elif question.startswith("7. Views of each Channel"):
    query7='''select channel_name as channelname, views as totalviews from channels'''
    cursor.execute(query7)
    t7=cursor.fetchall()
    conn.commit()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)


elif question.startswith("8. Videos Published in the year of 2022"):
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    conn.commit()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)  

elif question.startswith("9. Average duration of all videos in each channel"):
    query9 = '''
    SELECT channel_name AS channelname, AVG(IFNULL(duration, 0)) AS averageduration
    FROM videos
    WHERE duration IS NOT NULL
    GROUP BY channel_name
'''

    # Execute the query
    cursor.execute(query9)

    # Fetch the results
    t9 = cursor.fetchall()

    # Commit the transaction (optional, as it's a SELECT query)
    conn.commit()

    # Create a DataFrame from the results
    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

    # Convert average duration to string
    df9["averageduration"] = df9["averageduration"].astype(str)

    # Display the DataFrame in Streamlit
    st.write(df9)

elif question.startswith("10. Videos with highest number of comments"):
    query10='''select title as videotitle, channel_name as channelname, comments as comments from videos where comments
                is not null order by comments desc'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    conn.commit()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)
