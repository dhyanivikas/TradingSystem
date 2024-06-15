from gtts import gTTS
import os
from playsound import playsound

# Set the text you want to convert to speech
def playSoundBite(text):
    # Convert text to speech
    tts = gTTS(text=text, lang='en')
    # Save the audio file
    audio_file = "output.mp3"
    tts.save(audio_file)
    # Play the audio file
    playsound(audio_file)
    # Optionally, remove the audio file after playing
    os.remove(audio_file)

