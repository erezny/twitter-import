
FROM node:5.2-slim

MAINTAINER Elliott Rezny <erezny@gmail.com>

# Define mountable directories.
VOLUME ["/data/server"]

# Define working directory.
WORKDIR /data/server

# Expose ports.
EXPOSE 80

# Run npm install before building docker image
RUN mkdir /data/app
ADD ./ /data/app
RUN NPM_CONFIG_LOGLEVEL=warn npm install -g forever

ENTRYPOINT ["forever", "/data/app/app.js"]
