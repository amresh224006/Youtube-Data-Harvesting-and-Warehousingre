from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def Api_connect():
    Api_id="AIzaSyBsuOPAIDqUHevi7o-C7h0EbBgSBNuCMM4"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_id)
    return youtube
youtube=Api_connect()

def get_channel_info(channel_id):

    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response1=request.execute()

    for i in  range (0,len(response1['items'])):
        data=dict(Channel_name=response1["items"][i]['snippet']['title'],
                Channel_Id=response1["items"][i]['id'],
                Subscription_Count=response1["items"][i]['statistics']['subscriberCount'],
                Views=response1["items"][i]['statistics']['viewCount'],
                Total_Videos=response1["items"][i]['statistics']['videoCount'],
                Playlist_Id=response1["items"][i]['contentDetails']['relatedPlaylists']['uploads'],
                Channel_Description=response1["items"][i]['snippet']['description'])
    return data
                

def get_playlist_info(channel_id):
        All_data=[]
        next_page_token=None
        next_page=True
        
        while next_page:

                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                        )
                response=request.execute()

                for item in response['items']:
                        data=dict(PlaylistId=item['id'],
                                Title=item['snippet']['title'],
                                ChannelId=item['snippet']['channelId'],
                                ChannelName=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                VideoCount=item['contentDetails']['itemCount'])
                        All_data.append(data)
                next_page_token=response.get('nextPageToken')        
                if next_page_token is None:
                     next_page=False   
        return  All_data    


def get_channel_videos(channel_id):
        video_ids=[]
        res=youtube.channels().list(id=channel_id,part='contentDetails').execute()
        playlist_id=res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_Page_token=None
        while True:
            res=youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=50,pageToken=next_Page_token).execute()
            for i in range(len(res["items"])):
                video_ids.append(res["items"][i]["snippet"]["resourceId"]["videoId"])
            next_Page_token=res.get('nextPageToken')    

            if next_Page_token is None:
                break
        return video_ids    


def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
             part="snippet,contentDetails,statistics",
             id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                    Channel_Id=item["snippet"]['channelId'],
                    Video_Id=item["id"],
                    Title=item["snippet"]["title"],
                    Tags=item['snippet'].get("tags"),
                    Description=item['snippet'].get("description"),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item["contentDetails"]["duration"],
                    Likes=item['statistics'].get("likeCount"),
                    Views=item['statistics'].get("viewCount"),
                    Comments=item['statistics'].get("commentCount"),
                    Favorite_Count=item["statistics"]["favoriteCount"],
                    Definition=item["contentDetails"]["definition"],
                    Caption_Status=item["contentDetails"]["caption"])
            
                        
            video_data.append(data)
    return video_data  


def get_comment_info(video_ids):
    Comment_Information=[]
    try:
        for video_id in video_ids:
                request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
                response5=request.execute()

                for item in response5['items']:
                    comment_information=dict(Comment_Id=item[ 'snippet']['topLevelComment']['id'],
                            Video_Id=item[ 'snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                    Comment_Information.append(comment_information)

                    
    except:
        pass   
    return Comment_Information   


client=pymongo.MongoClient("mongodb+srv://amresh007:amresh07@guvi.rhquxoi.mongodb.net/?retryWrites=true&w=majority&appName=Guvi")
db=client["Youtube_data"]
                        
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_info(channel_id)
    vi_ids=get_channel_videos(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})

    return "upload completed successfully"


def channels_table():

    mydb=psycopg2.connect(host="localhost",user="postgres",
                        password="amresh07",database="youtube_data",
                        port="5433")
    cursor=mydb.cursor()

    drop_query="drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscription_Count bigint,
                                                            Views bigint,
                                                            Total_video int,
                                                            Playlist_Id varchar(100),
                                                            Channel_Description text )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("channel table already created")


    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Views,
                                            Total_videos,
                                            Playlist_Id,
                                            Channel_Description)
                                            
                                            VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_name'],
                row['Channel_id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_videos'],
                row['Playlist_Id'],
                row['Channel_Description'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            st.write("channels values are already inserted") 


def playlists_table():

    mydb=psycopg2.connect(host="localhost",user="postgres",
                        password="amresh07",database="youtube_data",
                        port="5433")
    cursor=mydb.cursor()

    drop_query="drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists playlists(PlaylistId varchar(100) primary key,
                                                        Title varchar(80) ,
                                                            ChannelId varchar(100),
                                                            ChannelName varchar(100),
                                                            PublishedAt timestamp,
                                                            VideoCount int)'''

                        
        cursor.execute(create_query)
        mydb.commit()
    except:
         st.write("Playlists Table already created")    

    
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    pl_list=[]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
          for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df=pd.DataFrame(pl_list)

    for index,row in df.iterrows():
        insert_query='''insert into playlist(PlaylistId,
                                                Title,
                                                ChannelId,
                                                ChannelName,
                                                PublishedAt,
                                                VideoCount
                                                )

                            
                                                VALUES(%s,%s,%s,%s,%s,%s)'''
            
        values=(row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount']
                )
        try:

            cursor.execute(insert_query,values)
            mydb.commit()
        except:
             st.write("Playlist values are already inserted")  


def videos_table():

        mydb=psycopg2.connect(host="localhost",user="postgres",
                                password="amresh07",database="youtube_data",
                                port="5433")
        cursor=mydb.cursor()
        
        drop_query="drop table if exists videos"
        cursor.execute(drop_query)
        mydb.commit()
        try:
            create_query='''create table if not exists videos(Channel_Name varchar(100) ,
                                                        Channel_Id varchar(100) ,
                                                            Video_Id varchar(30) primary key ,
                                                            Title varchar(150),
                                                            Tags text,
                                                            Description text,
                                                            Published_Date timestamp,
                                                            Duration interval,
                                                            Views bigint,
                                                            Likes bigint,
                                                            Comments int,
                                                            Favorite_Count int,
                                                            Definition varchar(10),
                                                            Caption_Status varchar(50) )'''
            cursor.execute(create_query)
            mydb.commit()
        except:
             st.write("Videos Table already created")    
        

        vi_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                  vi_list.append(vi_data["video_information"][i])

        df2=pd.DataFrame(vi_list)
        for index,row in df2.iterrows():
                insert_query='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
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
                                                        
                                                        

                        
                                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']
                        )

                try:
                    cursor.execute(insert_query,values)
                    mydb.commit()
                except:
                     st.write("Videos values already inserted in the table")      


def comments_table():

        mydb=psycopg2.connect(host="localhost",user="postgres",
                                password="amresh07",database="youtube_data",
                                port="5433")
        cursor=mydb.cursor()
        drop_query="drop table if exists comments"
        cursor.execute(drop_query)
        mydb.commit()
        try:
                create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(100),
                                                                Comment_Text text,
                                                                Comment_Author varchar(150),
                                                                Comment_Published timestamp
                                                                )'''

                            
                cursor.execute(create_query)
                mydb.commit()
        except:
           st.write("Comments Table already created")


        com_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                  com_list.append(com_data["comment_information"][i])

        df3=pd.DataFrame(com_list)


        for index,row in df3.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published)
                                                        
                                                        
                                                        
                                                        

                        
                                                VALUES(%s,%s,%s,%s,%s)'''

                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        
                       
                        )

                try:
                    cursor.execute(insert_query,values)
                    mydb.commit()
                except:
                     st.write("This comments are already exist in comments table")    


def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    
    return "Tables Created Successfully"


def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table=st.dataframe(ch_list)
    return channels_table


def show_playlists_table():
  
  db=client["Youtube_data"]
  coll1=db["channel_details"]
  pl_list=[]
  for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
          pl_list.append(pl_data["playlist_information"][i])

  playlists_table=st.dataframe(pl_list)
  return playlists_table


def show_videos_tables():
  vi_list=[]
  db=client["Youtube_data"]
  coll2=db["channel_details"]
  for vi_data in coll2.find({},{"_id":0,"video_information":1}):
          for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

  videos_table=st.dataframe(vi_list)

  return videos_table


def show_comments_table():
  com_list=[]
  db=client["Youtube_data"]
  coll3=db["channel_details"]
  for com_data in coll3.find({},{"_id":0,"comment_information":1}):
          for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

  comments_table=st.dataframe(com_list)

  return comments_table


with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

channel_id=st.text_input("Enter the Channel id")
channels=channel_id.split(',')
channels=[ch.strip() for ch in channels if ch]

if st.button("collect and store data"):
    for channel in channels:
        ch_ids=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel_id in ch_ids:
            st.success("channel details of the given channel id: " + channel + " already exists")
        else:
            output=channel_details(channel)
            st.success(output)

if st.button("migrate to sql"):
    display=tables()
    st.success(display)    

show_table=st.radio("select the table for view",(":red[channels]",":blue[playlists]",":green[videos]",":yellow[comments]"))

if show_table==":red[channels]":
    show_channels_table()

elif show_table==":blue[playlists]":
    show_playlists_table()

elif show_table==":green[videos]":
    show_videos_tables()

elif show_table==":yellow[comments]":
    show_comments_table()     



mydb=psycopg2.connect(host="localhost",user="postgres",
                    password="amresh07",database="youtube_data",
                    port="5433")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. What are the names all the videos and the channel name",
                                              "2. which channels have the most number of videos and how many videos do they have",
                                              "3. What are the top 10 most viewed videos and their respective channels",
                                              "4. How many comments were made on rach video,and what are their corresponding video names",
                                              "5. which videos have the highest number of likes,and what are their corresponding channel names",
                                              "6. What is the total number of likes for each video,and what are their corresponding video names",
                                              "7. What are the total numbers of views of each channels,and what are their corresponding channel names",
                                              "8. What are the names of all the channels that have published videos in the year of 2022",
                                              "9. What is average duration of all videos in each channel,and what are their corresponding channel names",
                                              "10. Which videos have the highest number of comments,and what are their corresponding channel names"))

if question == "1. What are the names all the videos and the channel name":
    query1 = "select Title as videos, Channel_Name as from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1,columns=["Videos Title","Channel Name"]))

elif question =="2. which channels have the most number of videos and how many videos do they have":
    query2= "select Channel_Name as ChannelName, Total_Videos  as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2,columns=["ChannelName","NO of Videos"]))

elif question =="3. What are the top 10 most viewed videos and their respective channels":
    query3 = "select Views as views, Channel_Name as ChannelName,Title as VideoTitle from videos where view is not null order by Views desc limit 10;"
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    st.write(pd.DataFrame(t3,columns=["Views","channel Name","video title"]))

elif question =="4. How many comments were made on rach video,and what are their corresponding video names":
    query4 = "select Comments as No_comments, Title as from VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4,columns=["No of Comments","Video Title"]))

elif question =="5. which videos have the highest number of likes,and what are their corresponding channel names":
    query5= "select Title as VideoTitle, Channel_Name as ChannelName,Likes as LikesCount from videos where Likes is not null order by Likes desc;"
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    st.write(pd.DataFrame(t5,columns=["Videos Title","Channel Name","like count"]))


elif question == "6. What is the total number of likes for each video,and what are their corresponding video names":
    query6 = "select Likes as LikeCount,Title as VideoTitle from videos;"
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    st.write(pd.DataFrame(t6,columns=["like count","video title"]))    


elif question == "7. What are the total numbers of views of each channels,and what are their corresponding channel names":
    query7 = "select Channel_Name as ChannelName,Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7,columns=["channel name","total views"]))

elif question == "8. What are the names of all the channels that have published videos in the year of 2022":
    query8 = "select Title as Video_Title, Published_Date as VideoRelease,Channel_Name as videos where extract(year from Published_Date )=2022;"
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name","Videos Published on","ChannelName"]))

elif question == "9. What is average duration of all videos in each channel,and what are their corresponding channel names":
    query9 = "select Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9=pd.DataFrame(t9,columns=["ChannelTitle","Average Duration"])
    t9=[]
    for index,row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str=str(average_duration)
        t9.append({"Channel Title": channel_title,"Average Duration":average_duration_str})
    st.write(pd.DataFrame(t9))    


elif question == "10. Which videos have the highest number of comments,and what are their corresponding channel names":
    query10 = "select Title as VideoTitle, Channel_Name as ChannelName,Comments as Comments from videos where Comments is not null order by Comments desc;"
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10,columns=["Videos Title","Channel Name","No of Comments"]))

        
        


