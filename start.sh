nohup gunicorn -c Config.py Server:app >log 2>&1 &
