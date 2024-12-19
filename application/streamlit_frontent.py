import streamlit as lt
import time
import json
from streamlit_backend import *
from streamlit_lottie import st_lottie
from streamlit.components.v1 import html


class Streamlit_UI():
    def __init__(self):

        self._backend = BackEnd()

        if "main" not in st.session_state:
            st.session_state.main = None
    
    """ ======================================= MAIN UI ======================================= """
    def display_main_UI(self):
        # animation
        spotify_animation = "/app/animation/spotify_animation.gif"
        music_animation = "/app/animation/music_animation.gif"
        
        # images
        spotify_logo = "https://www.freepnglogos.com/uploads/spotify-logo-png/spotify-icon-black-17.png"
        listener = "/app/images/music.png"
        
        # ------ PAGE CONFIGURATION ------
        st.set_page_config(page_title = "Spotiy Music Recommendation System", page_icon= ":notes:", layout= "wide")
        
        # Removeing withespace from the top of the page
        st.markdown("""
        <style>
        .css-18e3th9 { padding-top: 0rem; padding-bottom: 10rem; padding-left: 5rem; padding-right: 5rem; }
        .css-1d391kg { padding-top: 3.5rem; padding-right: 1rem; padding-bottom: 3.5rem; padding-left: 1rem; }
        
        /* Style for buttons */
        .stButton > button {
            width: 120px /* Set width of button to 100% */
            font-size: 20px; /* Set font size of button */
            background-color: green;
            color: #FFFFFF;
            border: none; /* Remove default border */
            border-radius: 5px; /* Add rounded corners */
            transition: background-color 0.3s; /* Smooth transition for background color */
        }

        .stButton > button:hover {
            background-color: black; /* Change background color on hover */
            color: #FFFFFF;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Button config
        m = st.markdown("""
        <style>
        div.stButton > button:first-child {
            background-color: green;
            color:#000000;
        } 
        div.stButton > button:hover {
            background-color: black;
            color:#FFFFFF;
            }
        </style>""", unsafe_allow_html=True)
        
        ## ------ WEBPAGE CODE ------
        page_bg = """
        <style>
        [data-testid="stAppViewContainer"] {
            background-image: url("https://i.pinimg.com/736x/10/26/9e/10269ec42b4b3aa81bef815f8bdf521f.jpg");
            background-size: cover;
            background-attachment: fixed; /* Giữ background đứng yên */
            background-position: center;
            background-repeat: no-repeat;
            opacity: 0.8; /* Giảm opacity để chữ nổi bật */
        }

        /* Tạo overlay màu mờ */
        [data-testid="stAppViewContainer"]::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.5); /* Màu đen mờ phủ lên */
            z-index: -1; /* Chìm xuống dưới nội dung */
        }

        /* Làm nổi bật chữ */
        h1, h2, h3, h4, h5, p, a {
            color: #FFFFFF !important; /* Màu chữ trắng */
            text-shadow: 1px 1px 2px black; /* Thêm viền mờ đen cho chữ */
        }
        </style>
        """
        st.markdown(page_bg, unsafe_allow_html=True)
  
        # Title and intro section
        # Heading
        heading_animation = """<p style="text-align: center; font-size: 60px;"><b>Welcome to Spotify Music Recommendation System 🎵</b></p>"""
        # Intro lines
        intro_para = """
        <p style = "font-size: 24px;">
        This recommendation system, along with the website, has been created by our team <span style="font-size:120%"><b>5T</b></span> as part of the Final Project for Python for Data Science Course.
        <br> <br>
        To try out the algorithms, you have two options for searching songs that you can find by <b>name of song </b> or you can find by <b> mood and genres </b> and our system will analyse various attributes such as artist, artist's genres, audio features, and more to recommend you songs that we hope you might like. <br> <br>
        Through our project, if you want to know more the details of the project, you can visit our link in <a href = https://github.com/Swuzz123/Big-Data-for-Music-Recommendation-System> <i>here</i></a> to have a more insightful and comprehensive view of our project,
        also better understand how we operate the project.
        </p>"""
        
        # ------ INTRODUCTION ------
        with st.container():
            left_col, right_col = st.columns([1, 9])
            with left_col:
                st.image(spotify_animation, use_container_width= True)
            with right_col:
                st.markdown(heading_animation, unsafe_allow_html= True)
        
        st.title("Introduction")
        with st.container():
            left_col, right_col = st.columns([1.4, 1])
            with left_col:
                st.markdown(intro_para, unsafe_allow_html = True)
            with right_col:
                st.image(listener, use_container_width = False)
                
        # ------ BUTTON OPTION ------
        st.title("Your option in here")
        line1 = """<p style = "font-size: 22px;"> 
        Explore your favourite artists or their genres by opting for an artist or an artist's genre. The <b> Recommend songs by artist, track, album </b> button will show you
        the relevant songs based on an artist or a song you search. The <b> Recommend songs by mood and genres </b> button will show you the songs based on the current mood and favourtite genre you select.
        </p> """
        st.markdown(line1, unsafe_allow_html = True)

        with st.container():
            left_col, middle_col, right_col = st.columns([3, 2.5, 2])

            with left_col:
                if st.button("Recommend songs by artist, track, album"):
                    st.session_state.search_page = {}
                    st.rerun()
            with middle_col:
                if st.button("Recommend songs for your Mood"):
                    st.session_state.search_by_mood = {}
                    st.rerun()
            with right_col:
                if st.button("Dash Board about artists, tracks, and albums"):
                    st.session_state.dashboard_link = "https://apc.safelink.emails.azure.net/redirect/?destination=https%3A%2F%2Fapp.powerbi.com%2FRedirect%3Faction%3DOpenLink%26linkId%3DpcEcnytrbo%26ctid%3D40127cd4-45f3-49a3-b05d-315a43a9f033%26pbi_source%3DlinkShare_meo&p=bT1jODA2NDFjMS04M2NhLTQ4MjItOTg1Ny1mZjUwZWM0Nzg5MGUmdT1hZW8mbD1SZWRpcmVjdA%3D%3D"
                    st.rerun()
        
        ##  ---- TOP 10 SONGS OF TOP 10 ARTIST RECOMMEND FOR USERS ----    
        st.markdown("""
        <style>
        .artist-image {
            width: 150px; /* Đặt chiều rộng cố định */
            height: 150px; /* Đặt chiều cao cố định */
            object-fit: cover; /* Đảm bảo hình ảnh được cắt đều */
            border-radius: 5%; /* Tùy chỉnh nếu muốn làm bo tròn */
            margin: 10px; /* Tạo khoảng cách giữa các hình ảnh */
        }
        </style>
        """, unsafe_allow_html=True)      

        top_tracks = self._backend.read_top_10_tracks()

        st.title("Top 10 songs from the top artists")
        if top_tracks:
            for row in range(2):
                cols = st.columns(5)
                for col, track in zip(cols, top_tracks[row*5 : (row + 1)*5]):
                    with col:
                        st.markdown(
                            f'<img class="artist-image" src="{track["LINK_IMAGE"]}" alt="Track Image">',
                            unsafe_allow_html=True
                        )
                        st.write(f"**{track['TRACK_NAME']}**")
                        st.caption(f"by {track['ARTIST_NAME']}")     
        
        # ------ FOOTER ------
        footer_style = """
        <style>
        .footer {
            text-align: center;
            font-size: 30px;
            margin-top: 30px;
            color: white;
        }
        .subfooter {
            text-align: center;
            font-size: 24px;
            margin-top: 10px;
            color: white;
        }
        a {
            color: #1DB954;
            text-decoration: none;
            font-weight: bold;
        }
        </style>
        """

        footer_animation = '<div class="footer"><b>Thanks for using our system to listen 🎵</b></div>'
        subfooter = '<div class="subfooter">Save tracks, follow artists and build your own playlists on main page <a href="https://www.spotify.com">Spotify</a>. All for free.</div>'

        st.markdown(footer_style, unsafe_allow_html=True)
        st.markdown(footer_animation, unsafe_allow_html=True)
        st.markdown(subfooter, unsafe_allow_html=True)
            
    """ ===================================== SEARCH SONGS ===================================== """
    def search_page(self):
        # ------ DESIGN WEB APP ------
        # Back button in the top-left corner
        if st.button("🏠︎ Home"):
            del st.session_state.search_page
            st.rerun()
        # animation
        music_logo = "/app/animation/pandas-animation.gif"

        # Button config
        m = st.markdown("""
        <style>
        div.stButton > button:first-child {
            background-color: green;
            color:#000000;
        } 
        div.stButton > button:hover {
            background-color: black;
            color:#FFFFFF;
            }
        </style>""", unsafe_allow_html=True)

        search_by_name_bg = """
        <style>
        [data-testid="stAppViewContainer"] {
            background-image: url("https://i.pinimg.com/originals/58/68/6a/58686a5f8ec2aba9ece2b9a1583838b6.gif");
            background-size: cover;
            background-attachment: fixed; /* Giữ background đứng yên */
            background-position: center;
            background-repeat: no-repeat;
            opacity: 0.8; /* Giảm opacity để chữ nổi bật */
        }
        
        /* Tạo overlay màu mờ */
        [data-testid="stAppViewContainer"]::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.5); /* Màu đen mờ phủ lên */
            z-index: -1; /* Chìm xuống dưới nội dung */
        }

        /* Làm nổi bật chữ */
        h1, h2, h3, h4, h5, p, a {
            color: #FFFFFF !important; /* Màu chữ trắng */
            text-shadow: 1px 1px 2px black; /* Thêm viền mờ đen cho chữ */
        }
        </style>
        """
        st.markdown(search_by_name_bg, unsafe_allow_html=True)

        # Intro lines
        intro_para = """
        <p style = 'font-size: 40px;'><b>Explore your favourite artists, tracks, albums in here:</b></p>
        <p style="font-size: 24px;">  
        <br>  
        Imagine stepping into a world where every note, every beat, and every lyric is tailored just for you. 
        Whether you're chasing a calm melody to unwind, a fiery rhythm to fuel your energy, or a nostalgic tune to relive memories — music speaks when words can't.
        <br><br>  
        Let us take you on a journey where your favorite songs meet hidden gems, and every recommendation feels like a personal discovery — because music isn't just heard, it's felt. 
        <br>
        </p>
        """
        with st.container():
            left_col, right_col = st.columns([2, 1])
            with left_col:
                st.markdown(intro_para, unsafe_allow_html= True)
            with right_col:
                st.image(music_logo, use_container_width= True)
        
        # ------ RECOMMEND SONGS ------
        if 'song_name' not in st.session_state:
            st.session_state.song_name = None

        # Input song name
        if st.session_state.song_name is None:
            # Input for searching a song
            song_name = st.text_input("Search a song:")
            if song_name:
                st.session_state.song_name = song_name
                st.rerun()
        else:
            # Finding the song
            songs_found = self._backend.read_music_db(st.session_state.song_name, None)

            # Display the searching result
            if not songs_found:
                st.markdown("No songs found!")
                if st.button("Back"):
                        st.session_state.song_name = None
                        st.rerun()
            else:
                st.write("### Search results: ")

                # Make a radio button for users to choose songs
                selected_song = st.radio(
                    "Choose a song:",
                    options=[f"{song['TRACK_NAME']} - {song['ARTIST_NAME']}" for song in songs_found],
                    index=0,
                    key="selected_song_radio"
                )

                # Find the chosen song
                for song in songs_found:
                    if selected_song == f"{song['TRACK_NAME']} - {song['ARTIST_NAME']}":
                        chosen_song = song
                        st.session_state.selected_song = chosen_song
                        break

                # Display the details of the chosen song
                if 'selected_song' in st.session_state:
                    chosen_song = st.session_state.selected_song
                    st.write("## Selected Song Details 🎵")
                    picture_line, info_line = st.columns([1, 4])
                    with picture_line:
                        st.image(chosen_song['LINK_IMAGE'])
                    with info_line:
                        st.write(f"### {chosen_song['TRACK_NAME']}")
                        st.write(f"**Artist**: {chosen_song['ARTIST_NAME']}")
                        st.write(f"**Followers**: {chosen_song['FOLLOWERS']}")
                        st.write(f"**Spotify**: {chosen_song['URL']}")
                    if chosen_song['PREVIEW']:
                        st.audio(chosen_song['PREVIEW'])

                    # Display other recommendation songs
                    st.write("### Recommended Songs for You:")
                    recommend_songs = self._backend.rcm_songs_by_cbf(chosen_song['TRACK_ID'], chosen_song['ALBUM_ID'])
                    for rcm_song in recommend_songs:
                        st.write(f"**{rcm_song['TRACK_NAME']}** by {rcm_song['ARTIST_NAME']}")
                        st.image(rcm_song['LINK_IMAGE'], width=100)
                        if rcm_song['PREVIEW']:
                            st.audio(rcm_song['PREVIEW'])

                    # Back button: Clear selected song and reset search
                    if st.button("Back"):
                        st.session_state.song_name = None
                        del st.session_state.selected_song
                        st.rerun()

    """ ================================ RECOMMEND SONGS BY MOOD ================================ """
    def search_by_mood(self):
        # ------ DESIGN WEB APP ------
        # Back button in the top-left corner
        if st.button("🏠︎ Home"):
            del st.session_state.search_by_mood
            st.rerun()
            
        # animation
        music_logo = "/app/animation/music_animation.gif"
        
        # Button config
        m = st.markdown("""
        <style>
        div.stButton > button:first-child {
            background-color: green;
            color:#000000;
        } 
        div.stButton > button:hover {
            background-color: black;
            color:#FFFFFF;
            }
        </style>""", unsafe_allow_html=True)
        
        search_by_mood_bg = """
        <style>
        [data-testid="stAppViewContainer"] {
            background-image: url("https://i.pinimg.com/originals/7b/ec/58/7bec589365fbdb1a95649e22e3da05c3.gif");
            background-size: cover;
            background-attachment: fixed; /* Giữ background đứng yên */
            background-position: center;
            background-repeat: no-repeat;
            opacity: 0.8; /* Giảm opacity để chữ nổi bật */
        }
        
        /* Tạo overlay màu mờ */
        [data-testid="stAppViewContainer"]::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.5); /* Màu đen mờ phủ lên */
            z-index: -1; /* Chìm xuống dưới nội dung */
        }

        /* Làm nổi bật chữ */
        h1, h2, h3, h4, h5, p, a {
            color: #FFFFFF !important; /* Màu chữ trắng */
            text-shadow: 1px 1px 2px black; /* Thêm viền mờ đen cho chữ */
        }
        </style>
        """
        st.markdown(search_by_mood_bg, unsafe_allow_html=True)
        
        # Intro lines
        intro_para = """
        <p style = 'font-size: 40px;'><b>Music speaks to us in many ways, and your mood matters to us:</b></p>
        <p style="font-size: 24px;">  
        <br>  
        If today feels a bit <b>sad 😢</b>, let us bring you gentle and soothing melodies to lift your spirits and remind you that brighter days are ahead.  
        <br>  
        If you're <b>feeling happy 🥰</b>, we've got vibrant and energetic tracks to keep the joy flowing through your day.  
        <br>  
        And if your mood is <b>neutral 😐</b>, why not explore a curated list of balanced tunes to keep things mellow and enjoyable?  
        <br><br>  
        Let's fill your <b>mood and genres</b>. Our system will bring songs to confide with you.  
        <br>
        </p>
        """
        with st.container():
            left_col, right_col = st.columns([2, 1])
            with left_col:
                st.markdown(intro_para, unsafe_allow_html= True)
            with right_col:
                st.image(music_logo, use_container_width= True)
        
        # ------ RECOMMEND SONGS ------
        genres = st.text_input("Choose your favourite genres: ")
        mood = st.selectbox("How is your mood today!", ["Happy", "Sad", "Neutral"])
        
        if st.button("Submit"):
            with st.status("Searching songs...", expanded = True) as status:
                time.sleep(1)
                status.update(label = "Search Complete", state = "complete", expanded = False)
                
            if genres:
                st.session_state.search_by_mood = {'selected_song': []}
                recommend_songs = self._backend.rcm_songs_by_mood(mood, genres)
                for rcm_song in recommend_songs:
                    st.session_state.search_by_mood['selected_song'].append(rcm_song)
                st.session_state.current_index = 0 # Reset the position of the song
                st.rerun()
                
        # Display list of song if have
        if "selected_song" in st.session_state.search_by_mood:
            songs = st.session_state.search_by_mood['selected_song']
            if songs:
                current_song = songs[st.session_state.current_index]
                
                # Display the current song
                picture_col, info_col = st.columns([1,4])
                with picture_col:
                    st.image(current_song['LINK_IMAGE'])
                with info_col:
                    st.write(f"### {current_song['NAME']}")
                    st.write(f"**Artist**: {current_song['ARTIST_NAME']}")
                    st.write(f"**Spotify**: {current_song['URL']}")
                st.audio(current_song['PREVIEW'])

                # Navigate other songs 
                col1, col2, col3 = st.columns([1, 2, 1])
                with col1:
                    if st.button("Previous", key="prev"):
                        if st.session_state.current_index > 0:
                            st.session_state.current_index -= 1
                            st.rerun()

                with col3:
                    if st.button("Next", key="next"):
                        if st.session_state.current_index < len(songs) - 1:
                            st.session_state.current_index += 1
                            st.rerun()
                        
                # Back to the main page
                if st.button("Back"):
                    del st.session_state.search_by_mood['selected_song']
                    del st.session_state.current_index
                    st.rerun()

    #======================================== Generate application ========================================
    def generate_application(self):
        if "search_page" in st.session_state:
            if "selected_song" in st.session_state.search_page:
                self.display_search()
            else:
                self.search_page()
            return
        
        if "search_by_mood" in st.session_state:
            self.search_by_mood()
            return 

        if "main" in st.session_state:
            self.display_main_UI()
            
            return 