# Specify the basic container image
FROM python:3

# Set the default directory where applications will be installed
WORKDIR /usr/src/app

# Copy files locally to the container file system
COPY . .

# install dependency
RUN pip install -r requirements.txt
RUN pip install .

# run flask app via gunicorn
CMD ["gunicorn", "flaskapp.app:app", "-c", "./gunicorn.conf.py"]