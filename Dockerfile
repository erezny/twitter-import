
FROM node:5.2-slim

MAINTAINER Elliott Rezny <erezny@gmail.com>

# Define working directory.
WORKDIR /data/app

# Expose ports.
EXPOSE 80

# Run npm install before building docker image
ADD ./ /data/app
RUN NPM_CONFIG_LOGLEVEL=warn npm install -g forever

ENTRYPOINT ["node", "/data/app/services.js"]
