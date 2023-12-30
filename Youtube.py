# Import the googleapiclient library
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
from PIL import Image


st.set_page_config(initial_sidebar_state="expanded")

# Streamlit layout for the first page
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select a page", ["Home", "Collecting Data","Data Visualization"])

if page == "Home":
    # To Display three images on the home page
    st.title("Home Page")
    
    image1 = 'C:/Users/sumat/OneDrive/Desktop/Capture1.JPG' 
    image2 = 'C:/Users/sumat/OneDrive/Desktop/Capture2.PNG' 
    image3 = 'C:/Users/sumat/OneDrive/Desktop/Capture3.PNG' 

    # Display the images
    st.image(Image.open(image1), use_column_width=True)
    st.image(Image.open(image2), use_column_width=True)
    st.image(Image.open(image3), use_column_width=True)

elif page == "Collecting Data":
    # Streamlit layout for the second page (Collecting Data)
    st.title("Collecting Data")

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
        # Fetch Channel data from MongoDB
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

            # Fetch Playlist data from MongoDB
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

        finally:
            cursor.close()
            conn.close()


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
            db = client["Youtube_data"] 
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

elif page == "Data Visualization":
    # Streamlit layout for the second page (Data Analysis)
    st.title("Data Visualization")

# streamlit

st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

# Custom CSS to change placeholder color
custom_css = """
<style>
    ::placeholder {
        color: green !important;
    }
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# Input in the placeholder
channel_id = st.text_input("", placeholder="Enter the channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]
st.text("")


if st.button("Collect and Store data to MongoDB"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)

st.text("")          
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
        
conn_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'youtube_data',
        'port': 3306,
    }

conn = mysql.connector.connect(**conn_params)
cursor = conn.cursor()

st.markdown("<h3 style='text-align:center;color:blue'>Youtube Channel Details</h3>",unsafe_allow_html=True)

table_query1 =  'select Channel_Name, Channel_Id,Subscribers,Total_Videos from channels'
cursor.execute(table_query1)
channel_details_table_data_1 = cursor.fetchall()
conn.commit()
channel_df_1=(pd.DataFrame(channel_details_table_data_1, columns=["Channel_Name", "Channel_Id", "Subscribers", "Total_Videos"]))

styled_channel_df_1 = channel_df_1.style.set_table_styles([
    {'selector': 'th', 'props': [('font-weight', 'bold'), ('color', 'black')]},
    {'selector': 'td', 'props': [('color', 'black')]}
])

# Display the table
st.table(styled_channel_df_1)

"Doughnut Chart: Channel Names vs Total Videos in percentage"

custom_colors = ['pink', 'skyblue', 'yellow', 'green', 'violet']

try:
    # Group by channel name and calculate the percentage of total likes
    channel_df_2 = channel_df_1.groupby('Channel_Name')['Total_Videos'].sum().reset_index()
    total_Videos_sum = channel_df_2['Total_Videos'].sum()
    channel_df_2['Total_Videos_Percentage'] = (channel_df_2['Total_Videos'] / total_Videos_sum) * 100

    # Plot the doughnut chart without explosion effect
    fig, ax = plt.subplots(figsize=(5, 5))  # Increase the figure size as needed
    wedges, texts, autotexts = ax.pie(
        channel_df_2['Total_Videos_Percentage'],
        labels=channel_df_2['Channel_Name'],  # Set labels to channel names
        autopct='%1.0f%%',
        startangle=90,
        colors=custom_colors,
        wedgeprops=dict(width=0.4),
        center=(0.5, 0.5)
    )
    ax.axis('equal')  # Equal aspect ratio ensures the doughnut chart is circular.

    # Create a legend based on the categories (Channel Names)
    ax.legend(channel_df_2['Channel_Name'], title="Channels", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

    # Adjust layout before displaying the chart
    plt.tight_layout()

    # Display the chart
    st.pyplot(fig)

except Exception as e:
    st.error(f"An error occurred while plotting the doughnut chart: {e}")

#published date
conn_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'youtube_data',
        'port': 3306,
    }

conn = mysql.connector.connect(**conn_params)
cursor = conn.cursor()

st.markdown("<h3 style='text-align:center;color:blue'>Youtube Channel Details</h3>",unsafe_allow_html=True)

# Fetch data from the 'videos' table
table_query = '''SELECT channel_name, COUNT(*) as total_videos, EXTRACT(YEAR FROM published_date) as year 
                 FROM videos GROUP BY channel_name, year;'''
cursor.execute(table_query)
channel_videos_data = cursor.fetchall()
conn.commit()
# Create a DataFrame
channel_videos_df = pd.DataFrame(channel_videos_data, columns=["Channel Name", "Total Videos", "Year"])

# Create a bar chart
fig = px.bar(channel_videos_df, x='Channel Name', y='Total Videos', color='Year',
             labels={'Total Videos': 'Total Videos Count', 'Year': 'Published Year'},
             title='Total Videos of Channel Names vs. Year')

# Show the chart
st.plotly_chart(fig)


#Select your questions and display answers
st.markdown('---')

st.markdown("<h5>Select Your Question</h5>",unsafe_allow_html=True)
question = st.selectbox('',                        
    ('1. Names of all videos and their corresponding channels',
     '2. Channels with most number of videos',
     '3. Top 10 most viewed videos and their respective channels',
     '4. Comments in each video and their corresponding video names',
     '5. Videos in highest number of likes and their corresponding channels names',
     '6. Total likes for all videos and corresponding video names',
     '7. Total views of each channel and their corresponding channel names',
     '8. Name of all channels and videos published in the year 2022',
     '9. Average duration of all videos in each channel and their corresponding channels names',
     '10. highest number of comments in video and their corresponding channels names'),)

if question == '1. Names of all videos and their corresponding channels':
    query1 = "select title, channel_Name from videos;"
    cursor.execute(query1)
    t1=cursor.fetchall()
    conn.commit()
    df=pd.DataFrame(t1, columns=["Video Title", "Channel Name"])
    # To display the DataFrame with increased row length
    st.dataframe(df.style.set_table_styles([
    {'selector': 'td', 'props': [('max-width', '300px')]}  # To adjust the max-width as needed
     ]))

elif question == '2. Channels with most number of videos':
    query2 = "select channel_name,total_videos from channels order by total_videos desc;"
    cursor.execute(query2)
    t2=cursor.fetchall()
    conn.commit()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No of Videos"]))

elif question == '3. Top 10 most viewed videos and their respective channels':
    query3 = '''select Views, Channel_Name,Title from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)    
    t3 = cursor.fetchall()
    conn.commit()
    st.write(pd.DataFrame(t3, columns = ["Views","Channel Name","Video Title"]))

elif question == '4. Comments in each video and their corresponding video names':
    query4 = "select Comments,Title from videos where Comments is not null;"
    cursor.execute(query4)    
    t4=cursor.fetchall()
    conn.commit()
    st.write(pd.DataFrame(t4, columns=["No of Comments","Video Title"]))

elif question == '5. Videos in highest number of likes and their corresponding channels names':
    query5 = '''select Title, Channel_Name, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)    
    t5 = cursor.fetchall()
    conn.commit()
    st.write(pd.DataFrame(t5, columns=["Video Title","Channel Name","Likecount"]))

elif question == '6. Total likes for all videos and corresponding video names':
    query6 = '''select likes,title from videos;'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    conn.commit()
    st.write(pd.DataFrame(t6, columns=["Likecount","Video Title"]))

elif question == '7. Total views of each channel and their corresponding channel names':
    query7 = "select Channel_Name, Views from channels;"
    cursor.execute(query7)    
    t7=cursor.fetchall()
    conn.commit()
    st.write(pd.DataFrame(t7, columns=["Channel Name","Totalviews"]))

elif question == '8. Name of all channels and videos published in the year 2022':
    query8 = '''select title, published_date, channel_name from videos 
                where extract(year from published_date) = 2022;'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    conn.commit()
    st.write(pd.DataFrame(t8,columns=["Video Title","Published_Date","Channel Name"]))

elif question == '9. Average duration of all videos in each channel and their corresponding channels names':
    query9 =  "SELECT channel_name, AVG(Duration) FROM videos GROUP BY channel_name;"
    cursor.execute(query9)
    t9=cursor.fetchall()
    conn.commit()
    t9 = pd.DataFrame(t9, columns=["Channel_Name", "Average_duration"])
    T9=[]
    for index, row in t9.iterrows():
        channel_name = row['Channel_Name']
        average_duration = row['Average_duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel_Title": channel_name ,  "Average_Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. Highest number of comments in video and their corresponding channels names':
    query10 = '''select title, channel_name, comments from videos 
                       where comments is not null order by comments desc;'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    conn.commit()
    st.write(pd.DataFrame(t10, columns=["Video Title","Channel Name","Comments"]))

