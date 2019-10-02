from app import app
import time

# The port number should be the same as the front end
# try:
# app.run(host='127.0.0.1', port = 9950, use_reloader=False, debug=True)
app.run(host='0.0.0.0', port = 9950, use_reloader=False, debug=True)
# except:
#     print("Some thing wrong!")






