FROM ubuntu:xenial
MAINTAINER ops @opentargets <ops@opentargets.org>

# Install required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends python-software-properties \
    build-essential \
    supervisor \
    python-dev \
    python \
    wget \
    curl \
    git \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*
	
RUN wget --no-check-certificate https://bootstrap.pypa.io/get-pip.py && \
    python get-pip.py


# setup
RUN mkdir -p /var/www/app \
    /var/www/app-conf \
    /var/log/supervisor 
	
#copy configuration files
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

WORKDIR /var/www/app

#copy requirements and install them
COPY requirements.txt /var/www/app
RUN pip install -r /var/www/app/requirements.txt

#copy the rest of the code (excluding what's in .dockerignore)
COPY . /var/www/app

#declare app port
EXPOSE 80 443 8080 8009

#define entrypoint
#COPY docker/docker-entrypoint.sh /
#RUN chmod +x /docker-entrypoint.sh
#ENTRYPOINT ["/docker-entrypoint.sh"]

#run supervisor to run uwsgi to run the flask app
CMD ["supervisord"]
