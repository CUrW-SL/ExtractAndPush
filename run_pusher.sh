#!/usr/bin/env bash

# Print execution date time
echo `date`

# Change directory into where email_notifer.py script is located.
echo "Changing into ~/data-pusher"
cd /home/uwcc-admin/data-pusher
echo "Inside `pwd`"

# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install PyMySQL using pip.
if [ ! -f "pusher.log" ]
then
    echo "Installing pytz"
    pip install pytz
    echo "Installing mysqladapter"
    pip install git+https://github.com/gihankarunarathne/CurwMySQLAdapter.git -U
fi

# Run email_notifier.py script.
echo "Running Pusher.py. Logs Available in notifier.log file."
python Pusher.py >> pusher.log

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
