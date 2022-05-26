## CODE TO DECRYPT AND DECOMPRESS 

import zlib
from cryptography.fernet import Fernet
import json

key = "sFmAIBsWchNS5jew5T1NGw3o_JLIP--88pK8T1tcdkg="

fernet = Fernet(key)

data = 'gAAAAABij8g4OWGEevWZqt-WbVWylD6k4wHb5esFglbJ1nYVwDCO3P1pN8K-ZdFyNM-nP18RSFenPWcOQfdVrLtTE3VGqCQ7-Q=='

decrypted_data = fernet.decrypt(data.encode())

uncompressed_data = zlib.decompress(decrypted_data, -15)

json_data = json.loads(uncompressed_data)

print(json_data)