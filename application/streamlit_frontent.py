import streamlit as lt
import time
import json
from streamlit_backend import *
from streamlit_lottie import st_lottie
from streamlit.components.v1 import html
from PIL import Image
import base64
from io import BytesIO

class Streamlit_UI():
    def __init__(self):

        self._backend = BackEnd()

        if "main" not in st.session_state:
            st.session_state.main = None
    
    #======================================== MAIN UI ========================================
    def display_main_UI(self):
        
        # animation
        spotify_animation = "spotify_animation.gif"
        music_animation = "music_animation.gif"
        
        # images
        spotify_logo = "https://www.freepnglogos.com/uploads/spotify-logo-png/spotify-icon-black-17.png"
        casette = "listening.png"
        
        # ------ PAGE CONFIGURATION ------
        st.set_page_config(page_title = "Spotiy Music Recommendation System", page_icon= ":notes:", layout= "wide")
        
        # Removeing withespace from the top of the page
        st.markdown("""
        <style>
        .css-18e3th9 { padding-top: 0rem; padding-bottom: 10rem; padding-left: 5rem; padding-right: 5rem; }
        .css-1d391kg { padding-top: 3.5rem; padding-right: 1rem; padding-bottom: 3.5rem; padding-left: 1rem; }
        </style>""", unsafe_allow_html=True)
        
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
            background-image: url("https://i.pinimg.com/originals/80/c3/32/80c332329f7f9b40d3ec776130813859.gif");
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

        .stButton > button {
            background-color: #1DDA63;
            color: white;
            border-radius: 8px;
            font-size: 16px;
            font-weight: bold;
            transition: 0.3s;
        }

        .stButton > button:hover {
            background-color: #14B856; /* Màu đậm hơn khi hover */
        }
        </style>
        """

        st.markdown(page_bg, unsafe_allow_html=True)
  
        # Title and intro section
        # Heading
        heading_animation = "<p style = 'font-size: 70px;'><b>Welcome to Spotify Music Recommendation System 🎵</b></p>"
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
                  
        with st.container():
            left_col, right_col = st.columns([1.4, 1])
            with left_col:
                st.markdown(intro_para, unsafe_allow_html = True)
            with right_col:
                st.image(casette, use_container_width = False)
                
        # ------ BUTTON OPTION ------
        st.title("Your option in here")
        line1 = """<p style = "font-size: 22px;"> 
        Explore your favourite artists or their genres by opting for an artist or an artist's genre. The <b> Recommend songs by artist, track, album </b> button will show you
        the relevant songs based on an artist or a song you search. The <b> Recommend songs by mood and genres </b> button will show you the songs based on the current mood and favourtite genre you select.
        </p> """
        st.markdown(line1, unsafe_allow_html = True)

        # --- TAB CONFIG ---
        with st.container():
            left_col, middle_col, right_col = st.columns([4, 1, 4])

            with left_col:
                if st.button("Recommend songs by artist, track, album"):
                    st.session_state.search_page = {}
                    st.rerun()
            with right_col:
                if st.button("Recommend songs for your Mood"):
                    st.session_state.search_by_mood = {}
                    st.rerun()
        
        st.write("Chỗ này là mốt sẽ để 5-10 bài mà của 10 thằng nghệ sĩ top thế giới ra cho người ta ấy. Nếu m thấy ok thì để không thì thôi. Tại t thấy hơi trống")
        ##  ---- TOP 5 SONGS OF TOP 5 ARTIST RECOMMEND FOR USERS ----          
        # recommended_music_names, recommended_music_posters = recommend(selected_movie)
        # col1, col2, col3, col4, col5 = st.columns(5)
        # with col1:
        #     st.text(recommended_music_names[0])
        #     st.image(recommended_music_posters[0])
        # with col2:
        #     st.text(recommended_music_names[1])
        #     st.image(recommended_music_posters[1])

        # with col3:
        #     st.text(recommended_music_names[2])
        #     st.image(recommended_music_posters[2])
        # with col4:
        #     st.text(recommended_music_names[3])
        #     st.image(recommended_music_posters[3])
        # with col5:
        #     st.text(recommended_music_names[4])
        #     st.image(recommended_music_posters[4])
      
              
    #======================================== Search songs ========================================
    def search_page(self):
        song_name = st.text_input("Search a song:")
        artist_name = st.text_input("Search an artist: ")
        songs_found = self._backend.read_music_db(song_name, None)
        
        if not songs_found:
            st.markdown("No songs found!")
        else:
            st.write("### Search results: ")
            for song in songs_found:
                if(st.button(f"{song['TRACK_NAME']} - {song['ARTIST_NAME']}", key = song['TRACK_ID'])):
                    st.session_state.search_page['selected_song'] = song
                    st.rerun()

        if st.button("Back"):
            del st.session_state.search_page
            st.rerun()

    def display_search(self):
        song = st.session_state.search_page['selected_song']
        picture_line, info_line = st.columns([1,4])
        with picture_line:
            st.image(song['LINK_IMAGE'])

        with info_line:
            st.write(f"### {song['TRACK_NAME']}")
            st.write(f"**Artist**: {song['ARTIST_NAME']}")
            st.write(f"**Followers**: {song['FOLLOWERS']}")
            st.write(f"**Spotify**: {song['URL']}")
        if song['PREVIEW']: st.audio(song['PREVIEW'])

        recommend_songs = self._backend.rcm_songs_by_cbf(song['TRACK_ID'], song['ALBUM_ID'])
        for rcm_song in recommend_songs:
            st.write(rcm_song['TRACK_NAME'])
            st.write(rcm_song['ARTIST_NAME'])
            st.image(rcm_song['LINK_IMAGE'])
            if rcm_song['PREVIEW']: st.audio(rcm_song['PREVIEW'])
            
        if st.button("Back"):
            del st.session_state.search_page['selected_song']
            st.rerun()

    #======================================== Recommend songs by mood ========================================
    def search_by_mood(self):
        # Back button in the top-left corner
        if st.button("🏠︎ Home"):
            del st.session_state.search_by_mood
            st.rerun()
            
        genres = st.text_input("Choose your favourite genres: ")
        mood = st.selectbox("How is your mood today!", ["Happy", "Sad", "Neutral"])
        st.write("Your mood is: ", mood)
        
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
                    
    # def display_search_by_mood(self):
    #     songs = st.session_state.search_by_mood['selected_song']
    #     for song in songs:
    #         picture_line, info_line = st.columns([1,4])
            
    #         with picture_line:
    #             st.image(song['LINK_IMAGE'])

    #         with info_line:
    #             st.write(f"### {song['NAME']}")
    #             st.write(f"**Artist**: {song['ARTIST_NAME']}")
    #             st.write(f"**Spotify**: {song['URL']}")
    #         st.audio(song['PREVIEW'])

    #     if st.button("Back"):
    #         del st.session_state.search_by_mood['selected_song']
    #         st.rerun()


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