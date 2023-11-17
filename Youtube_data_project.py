import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

api_key='AIzaSyD3JyJaEqiOXMRDSBZ0z0AraUr9756uAvk'

youtube = googleapiclient.discovery.build("youtube","v3", developerKey=api_key) 

#CHANNEL DETAILS - 
def get_channel_info(ch_id):
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= ch_id
        )
    response = request.execute()

    for i in response['items']:
        
        data = dict(channel_Name=i['snippet']['title'],
               channel_Id=i['id'],
               channel_Subs = i['statistics']['subscriberCount'],
               channel_View=i['statistics']['viewCount'],
               Total_Videos = i['statistics']['videoCount'],
               chan_Des=i['snippet']['description'],
               playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data
   
#VIDEO_IDS
def get_video_ids(channel_id):   #to get all videos channel playlist id's

    video_ids=[]
    response = youtube.channels().list(id =channel_id,
                                       part='contentDetails').execute()
    playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'] 

    next_page_token= None

    while True:
        res1 = youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=playlist_Id,
                                        maxResults= 50,
                                        pageToken=next_page_token).execute() 

        for  i in range(len(res1['items'])):
            video_ids.append(res1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=res1.get('nextPageToken')

        if next_page_token is None:
            break 
    return video_ids

#VIDEO INFORMATIONS
def get_video_info(v_Ids):

    video_data=[]
    for video_id in v_Ids:
        request = youtube.videos().list(
                                        part='snippet, contentDetails, statistics',
                                        id=video_id

        )
        response=request.execute()

        for item in response['items']:    #item is for line by line calling and accessing the details
            data= dict(channel_name=item['snippet']['channelTitle'],
                       channel_Id = item['snippet']['channelId'],
                        video_id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Descirption = item['snippet'].get('description'),
                        Published_date=item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics']['likeCount'],
                        Dislikes = item['statistics'].get('dislikeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Fav_count= item['statistics']['favoriteCount'],
                        caption_status = item['contentDetails']['caption'])

            video_data.append(data)
    return video_data

#COMMENTS DETAILS -
def get_comment_info(cm_id):
    Comment_data=[]

    try:
        for video_id in cm_id:
            request = youtube.commentThreads().list(
                         part = 'snippet',
                         videoId=video_id,
                         maxResults = 50
                )
            response = request.execute()

            for item in response['items']:
                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                          video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_text= item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt'])

                Comment_data.append(data)

    except:
        pass
    return Comment_data 

#PLAYLISTS DETAILS - 
def get_playlist_info(channel_id):
    next_page_token = None
    Playlist_data=[]

    while True:

            request = youtube.playlists().list(
                      part='snippet, contentDetails',
                      channelId = channel_id,
                      maxResults=50,
                      pageToken = next_page_token

                )
            response = request.execute()

            for item in response['items']:
                data = dict(Playlist_Id = item['id'],
                            Title = item['snippet']['title'],
                            Channel_Id = item['snippet']['channelId'],
                            channel_Name=item['snippet']['channelTitle'],
                            Published_date = item['snippet']['publishedAt'],
                            Video_count = item['contentDetails']['itemCount'])

                Playlist_data.append(data)

            next_page_token = response.get('nextPageToken')

            if next_page_token is None:
                break
    return Playlist_data

    
#MONGODB CONNECTION - 
client = pymongo.MongoClient('mongodb://localhost:27017')
database_Name = client['Youtube_project']
Coll_Name = database_Name['Channel_Details']
    

#CHANNEL DETAILS - 
def chan_details(channel_id):
    Channel_Details = get_channel_info(channel_id)
    Playlist_Details = get_playlist_info(channel_id)
    Video_id = get_video_ids(channel_id)
    Video_Details=get_video_info(Video_id)
    Comment_Details= get_comment_info(Video_id)
    
    
    Coll_Name.insert_one({'Channel_Information':Channel_Details, 
                          'Playlist_Information':Playlist_Details,
                          'Video_Information':Video_Details,
                          'Comment_Information':Comment_Details})
    
    return 'Document Inserted Successfully!'

#CHANNEL TABLE - 
def channels_table():
    db = psycopg2.connect(host='localhost',
                               user='postgres',
                               password = 'vela',
                               database='youtube_details_db',
                               port='5432')

    cursor  = db.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    db.commit()


    try:
        create_query='''create table if not exists channels(channel_Name varchar(100),
                                                            channel_Id varchar(80) primary key,
                                                            channel_Subs bigint,
                                                            channel_View bigint,
                                                            Total_Videos int,
                                                            chan_Des text,
                                                            playlist_Id varchar(80))'''



        cursor.execute(create_query)
        db.commit()


    except:
        print('channels table has been created already')

    ch_list = []
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    # extracting the data from mongodb
    for ch_data in Coll_Name.find({},{'_id':0, 'Channel_Information':1}):
        ch_list.append(ch_data['Channel_Information'])

    df = pd.DataFrame(ch_list)

    for index,row in df.iterrows():  # iterating the data from mongodb and to transfer it to sql table
        ins_query = '''insert into channels(channel_Name ,
                                            channel_Id ,
                                            channel_Subs,
                                            channel_View,
                                            Total_Videos,
                                            chan_Des,
                                            playlist_Id )


                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['channel_Name'],
                  row['channel_Id'],
                  row['channel_Subs'],
                  row['channel_View'],
                  row['Total_Videos'],
                  row['chan_Des'],
                  row['playlist_Id'])

        try:
            cursor.execute(ins_query,values)
            db.commit()
            
        except:
            print('channel data are aldready inserted')

#PLAYLISTS TABLE - 
def playlist_table():
    db = psycopg2.connect(host='localhost',
                           user='postgres',
                           password = 'vela',
                           database='youtube_details_db',
                           port='5432')

    cursor  = db.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    db.commit()

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                         Title varchar(100),
                                                         Channel_Id varchar(100),
                                                         channel_Name varchar(100),
                                                         Published_date timestamp,
                                                         Video_count int)'''

    cursor.execute(create_query)
    db.commit()

    pl_list = []
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    # extracting the data from mongodb
    for pl_data in Coll_Name.find({},{'_id':0, 'Playlist_Information':1}):

        for p in range(len(pl_data['Playlist_Information'])): 

            pl_list.append(pl_data['Playlist_Information'][p])  #specifically getting individual playlist details

    df1 = pd.DataFrame(pl_list)

    for index,row in df1.iterrows():  # iterating the playlist data from mongodb and to transfer it to sql table
        ins_query = '''insert into playlists(Playlist_Id ,
                                            Title ,
                                            Channel_Id,
                                            channel_Name,
                                            Published_date,
                                            Video_count)

                                            values(%s,%s,%s,%s,%s,%s)'''        

        values = (row['Playlist_Id'],
                  row['Title'],
                  row['Channel_Id'],
                  row['channel_Name'],
                  row['Published_date'],
                  row['Video_count'])


        try:
            cursor.execute(ins_query,values)
            db.commit()


        except:
            print('playlists data are aldready inserted')

#VIDEOS TABLE - 

def videos_table():
    db = psycopg2.connect(host='localhost',
                           user='postgres',
                           password = 'vela',
                           database='youtube_details_db',
                           port='5432')

    cursor  = db.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    db.commit()


    create_query='''create table if not exists videos(channel_name varchar(100),
                                                      channel_Id varchar(100),
                                                      video_id varchar(30) primary key,
                                                      Title varchar(130),
                                                      Tags text,
                                                      Thumbnail varchar(200) ,
                                                      Descirption text,
                                                      Published_date timestamp,
                                                      Duration interval,
                                                      Views bigint,
                                                      Likes bigint,
                                                      Dislikes bigint,
                                                      Comments int,
                                                      Fav_count int,
                                                      caption_status varchar(50))'''


    cursor.execute(create_query)
    db.commit()
    
    vi_list = []
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    # extracting the data from mongodb
    for vi_data in Coll_Name.find({},{'_id':0, 'Video_Information':1}):

        for v in range(len(vi_data['Video_Information'])): 

            vi_list.append(vi_data['Video_Information'][v])  #specifically getting individual playlist details

    df2 = pd.DataFrame(vi_list)


    for index,row in df2.iterrows():  # iterating the playlist data from mongodb and to transfer it to sql table
            ins_query = '''insert into videos(channel_name,
                                              channel_Id,
                                              video_id,
                                              Title,
                                              Tags,
                                              Thumbnail,
                                              Descirption,
                                              Published_date,
                                              Duration,
                                              Views,
                                              Likes,
                                              Dislikes,
                                              Comments,
                                              Fav_count,
                                              caption_status)

                                              values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''        

            values = (row['channel_name'],
                      row['channel_Id'],
                      row['video_id'],
                      row['Title'],
                      row['Tags'],
                      row['Thumbnail'],
                      row['Descirption'],
                      row['Published_date'],
                      row['Duration'],
                      row['Views'],
                      row['Likes'],
                      row['Dislikes'],
                      row['Comments'],
                      row['Fav_count'],
                      row['caption_status']) 



            try:
                cursor.execute(ins_query,values)
                db.commit()


            except:
                print('videos data are aldready inserted')

#COMMENTS TABLE -

def comments_table():
    db = psycopg2.connect(host='localhost',
                           user='postgres',
                           password = 'vela',
                           database='youtube_details_db',
                           port='5432')

    cursor  = db.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    db.commit()

    create_query='''create table if not exists comments(comment_Id varchar(100) primary key,
                                                        video_Id varchar(50),
                                                        Comment_text text,
                                                        Comment_author varchar(200),
                                                        comment_published timestamp)'''

    cursor.execute(create_query)
    db.commit()

    co_list = []
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    # extracting the data from mongodb
    for co_data in Coll_Name.find({},{'_id':0,'Comment_Information':1}):

        for c in range(len(co_data['Comment_Information'])): 

            co_list.append(co_data['Comment_Information'][c])  #specifically getting individual playlist details

    df3 = pd.DataFrame(co_list)

    for index,row in df3.iterrows():  # iterating the playlist data from mongodb and to transfer it to sql table
        ins_query = '''insert into comments(comment_Id,
                                            video_Id,
                                            Comment_text,
                                            Comment_author,
                                            comment_published)

                                            values(%s,%s,%s,%s,%s)'''        

        values = (row['comment_Id'],
                  row['video_Id'],
                  row['Comment_text'],
                  row['Comment_author'],
                  row['comment_published'])



        try:
            cursor.execute(ins_query,values)
            db.commit()


        except:
            print('comments data are aldready inserted')


#TABLES CREATION -
def tables():
    
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return 'Tables are successfully created!'

#show_channel_table - 
def show_channel_table():
    
    ch_list = []
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    # extracting the data from mongodb
    for ch_data in Coll_Name.find({},{'_id':0, 'Channel_Information':1}):
        ch_list.append(ch_data['Channel_Information'])

    df = st.dataframe(ch_list)
    
    return df

#show_playlists_table - 
def show_playlists_table():
    pl_list = []
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    # extracting the data from mongodb
    for pl_data in Coll_Name.find({},{'_id':0, 'Playlist_Information':1}):

        for p in range(len(pl_data['Playlist_Information'])): 

            pl_list.append(pl_data['Playlist_Information'][p])  #specifically getting individual playlist details

    df1 = st.dataframe(pl_list)
    
    return df1

#show_videos_table - 
def show_videos_table():
    vi_list = []
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    # extracting the data from mongodb
    for vi_data in Coll_Name.find({},{'_id':0, 'Video_Information':1}):

        for v in range(len(vi_data['Video_Information'])): 

            vi_list.append(vi_data['Video_Information'][v])  #specifically getting individual playlist details

    df2 = st.dataframe(vi_list)
    
    return df2

#show_comments_table - 
def show_comments_table():
    co_list = []
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    # extracting the data from mongodb
    for co_data in Coll_Name.find({},{'_id':0,'Comment_Information':1}):

        for c in range(len(co_data['Comment_Information'])): 

            co_list.append(co_data['Comment_Information'][c])  #specifically getting individual playlist details

    df3 = st.dataframe(co_list)
    
    return df3


#STREAMLIT CODE - 

with st.sidebar:
    st.title(':white[Youtube Data Harvesting and Data Warehousing 💻]')
    st.write('------------------------------')
    st.header(":red[📜 Key skills take away:]")
    st.caption(':blue[✨Python scripting]')
    st.caption(':blue[✨Data collection from youtube API] ')
    st.caption(':blue[✨MongoDB]')
    st.caption(':blue[✨Postgresql migrating from MongoDB Data]')
    

channel_id = st.text_input('Enter the channel ID 👇 : ')
st.write('')

if st.button(':blue[Collect and store the data in MongoDB 📁]'):
    ch_ids=[]
    database_Name = client['Youtube_project']
    Coll_Name = database_Name['Channel_Details']

    for ch_data in Coll_Name.find({},{'_id':0, 'Channel_Information':1}):
        ch_ids.append(ch_data['Channel_Information']['channel_Id'])
        
    if channel_id in ch_ids:
        st.success('Data has been already inserted!👍')
    else:
        insert = chan_details(channel_id)
        st.success('Inserted successfully!🙌')
        
   
        
if st.button(':green[Migrate the MongoDB 📁 to SQL database 🖥]'):
    
    Table= tables()
    st.success(Table)
    
st.write('')
show_table = st.radio('SELECT THE TABLE FOR VIEW',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))
st.write('------------------------')

if show_table == 'CHANNELS':
    show_channel_table()
    
elif show_table == 'PLAYLISTS':
    show_playlists_table()
    
elif show_table == 'VIDEOS':
    show_videos_table()
    
elif show_table == 'COMMENTS':
    show_comments_table()
    
    
#STREAMLIT QUERY'S - 10 QUESTIONS-
#SQL CONNECTION
db = psycopg2.connect(host='localhost',
                       user='postgres',
                       password = 'vela',
                       database='youtube_details_db',
                       port='5432')

cursor  = db.cursor()
st.write('--------------------------cd')
questions = st.selectbox("Select your Questions:",('1. What are the names of all the videos and their corresponding channels?',
                                                   '2. Which channels have the most number of videos, and how many videos do they have?',
                                                   '3. What are the top 10 most viewed videos and their respective channels?',
                                                   '4. How many comments were made on each video, and what are their corresponding video names?',
                                                   '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                   '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                                   '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                                   '8. What are the names of all the channels that have published videos in the year 2022?',
                                                   '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                                   '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))



#questions  - query answers fetching from the videos table!
if questions=='1. What are the names of all the videos and their corresponding channels?':

    q1 = '''select Title as videos,channel_name as channelname from videos'''
    cursor.execute(q1)
    db.commit()

    tab1 = cursor.fetchall()
    df = pd.DataFrame(tab1,columns=['Video title','channel name'])
    st.write(df)



elif questions =='2. Which channels have the most number of videos, and how many videos do they have?':

    q2 ='''select channel_Name as channelname,Total_Videos as total_videos from channels 
                order by Total_Videos desc'''
    cursor.execute(q2)
    db.commit()

    tab2 = cursor.fetchall()
    df1 = pd.DataFrame(tab2,columns=['channel name','total number of videos'])
    st.write(df1)


elif questions =='3. What are the top 10 most viewed videos and their respective channels?':

    q3 = '''select Views as views,channel_name as channelname,Title as video_title from videos 
                where Views is not null order by Views desc limit 10'''
    cursor.execute(q3)
    db.commit()

    tab3 = cursor.fetchall()
    df2 = pd.DataFrame(tab3,columns=['views','channel name','video title'])
    st.write(df2)
    
elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':

    q4 = '''select Comments as no_of_comments, Title as video_title from videos
             where Comments is not null'''

    cursor.execute(q4)
    db.commit()

    tab4 = cursor.fetchall()
    df3 = pd.DataFrame(tab4,columns=['no_of_comments','video title'])
    st.write(df3)

elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':

    q5 = '''select Title as video_title, channel_name as channelname, Likes as like_counts from videos
                where Likes is not null order by Likes desc'''

    cursor.execute(q5)
    db.commit()

    tab5 = cursor.fetchall()
    df4 = pd.DataFrame(tab5,columns=['video title','çhannel name','Likes count'])
    st.write(df4)

elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':

    q6 = '''select Likes as like_counts, Title as video_title from videos'''

    cursor.execute(q6)
    db.commit()

    tab6 = cursor.fetchall()
    df5 = pd.DataFrame(tab6,columns=['Likes count','video title'])
    st.write(df5)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

    q7 = '''select channel_Name as channel_name, channel_View as views from channels '''

    cursor.execute(q7)
    db.commit()

    tab7 = cursor.fetchall()
    df6 = pd.DataFrame(tab7,columns=['channel name','views'])
    st.write(df6)

elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':

    q8 = '''select Title as video_title,  Published_date as video_release, channel_name as channelname from videos
                where extract(year from Published_date)= 2022'''

    cursor.execute(q8)
    db.commit()

    tab8 = cursor.fetchall()
    df7 = pd.DataFrame(tab8,columns=['video title','published date','channel name'])
    st.write(df7)

elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':

    q9 = '''select channel_name as channelname, AVG(Duration) as average_duration from videos group by channel_name'''

    cursor.execute(q9)
    db.commit()

    tab9 = cursor.fetchall()
    df8 = pd.DataFrame(tab9,columns=['channel name','average duration'])
    #st.write(df8)


    #creating an empty list to convert the timestamp to string 
    T9=[]
    for index,row in df8.iterrows():
        channel_title = row['channel name']
        average_duration = row['average duration']
        avg_dur_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avg_duration = avg_dur_str))
    dft=pd.DataFrame(T9)
    st.write(dft)
    
elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':

    q10 = '''select Title as video_title, channel_name as channelname, Comments as comments from videos 
                where Comments is not null order by Comments desc'''

    cursor.execute(q10)
    db.commit()

    tab10 = cursor.fetchall()
    df9 = pd.DataFrame(tab10,columns=['video title','channel name','comments'])
    st.write(df9)
    
 
    
















    
