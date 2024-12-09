import streamlit as lt
from streamlit_backend import *


class Streamlit_UI():
    def __init__(self):
        if "main" not in st.session_state:
            st.session_state.main = None

    #======================================== MAIN UI ========================================
    def display_main_UI(self):
        st.title("🎶 Welcome to Music Recommendation App! 🎶")
        st.subheader("Discover your favorite songs and artists here!")
        
        if st.sidebar.button("Search", use_container_width = True):
            st.session_state.search = {}
            st.rerun()

        if st.sidebar.button("Recommend Song for your Mood", use_container_width = True):
            st.session_state.search_by_mood = {}
            st.rerun()

            
    #======================================== Search songs ========================================
    def search_page(self):
        song_name = st.text_input("Search a song:")
        if song_name:
            data = search_track_Snowflake(song_name)
            if data.empty:
                st.markdown("No songs found!")
            else:
                st.write("### Search results: ")
                for _ , song in data.iterrows():
                    if(st.button(f"{song.TRACK_NAME} - {song.ARTIST_NAME}", key = song.TRACK_ID)):
                        st.session_state.search['selected_song'] = song.to_dict()
                        st.rerun()
        if st.button("Back"):
            del st.session_state.search
            st.rerun()

    def display_search(self):
        song = st.session_state.search['selected_song']
        picture_line, info_line = st.columns([1,4])
        with picture_line:
            st.image(song['LINK_IMAGE'])

        with info_line:
            st.write(f"### {song['TRACK_NAME']}")
            st.write(f"**Artist**: {song['ARTIST_NAME']}")
            st.write(f"**Followers**: {song['FOLLOWERS']}")
            st.write(f"**Spotify**: {song['URL']}")
        st.audio(song['PREVIEW'])

        if st.button("Back"):
            del st.session_state.search['selected_song']
            st.rerun()

    #======================================== Search songs by mood ========================================
    def search_by_mood(self):
        genres = st.text_input("Choose your favourite genres: ")
        mood = st.radio("Choose your mood today!", options = ["Happy", "Sad", "Neutral"])

        if st.button("Go!"):
            if genres:
                st.session_state.search_by_mood = {'selected_song': []}
                songs = search_rcm_mood_genres(mood, genres)
                for _ , song in songs.iterrows():
                    st.session_state.search_by_mood['selected_song'].append(song.to_dict())
                st.rerun()
                
    def display_search_by_mood(self):
        songs = st.session_state.search_by_mood['selected_song']
        for song in songs:
            picture_line, info_line = st.columns([1,4])
            
            with picture_line:
                st.image(song['LINK_IMAGE'])

            with info_line:
                st.write(f"### {song['NAME']}")
                st.write(f"**Artist**: {song['ARTIST_NAME']}")
                st.write(f"**Spotify**: {song['URL']}")
            st.audio(song['PREVIEW'])

        if st.button("Back"):
            del st.session_state.search_by_mood['selected_song']
            st.rerun()


    #======================================== Generate application ========================================
    def generate_application(self):
        if "search" in st.session_state:
            if "selected_song" in st.session_state.search:
                self.display_search()
            else:
                self.search_page()
            return
        
        if "search_by_mood" in st.session_state:
            if "selected_song" in st.session_state.search_by_mood:
                self.display_search_by_mood()
            else:
                self.search_by_mood()
            return


        if "main" in st.session_state:
            self.display_main_UI()

            return 
