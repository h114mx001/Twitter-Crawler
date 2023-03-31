#!/bin/bash
python3 app.py &
python3 crawler.py & 
wait -n 
exit $?

